import sys
import os
import logging
import asyncio
from .media_player import MediaPlayer, MediaPlayerType
from ..rewriter import Rewriter
from typing import Set, List
from asyncio import PriorityQueue
from datetime import datetime, timedelta
from functools import cached_property

class Plex(MediaPlayer):
    def __init__(self, now: datetime, rewriter: Rewriter, url: str, token: str, libraries: List[str] = [], users: List[str] = []):
        self.now: datetime  = now
        self.rewriter: Rewriter = rewriter
        self.url: str = url
        self.token: str = token
        self.libraries: Set[str] = set(libraries)
        self.users: Set[str] = set(users)
        
    @cached_property
    def __plex(self):
        return self.get_plex_server(self.token)
    
    @cached_property
    def __plex_servers(self):
        plex = self.__plex
        
        return [plex] + [plex.switchUser(u) for u in plex.myPlexAccount().users() if not self.users or u.username in self.users]
    
    def get_plex_server(self, token: str):
        try:
            from plexapi.server import PlexServer
        except ModuleNotFoundError:
            logging.error('Requirements Error: plexapi not installed. Please install using the command "pip install plexapi"')
            sys.exit(1)
            
        return PlexServer(self.url, token)
    
    @cached_property
    def not_watched_media(self) -> asyncio.Task[Set[str]]:
        def __populate(item, result):
            for media in item.media:
                for part in media.parts:
                    path = self.rewriter.on_source(part.file)
                    if os.path.exists(path):
                        logging.debug("[%s] Non-Watched %s: %s (%s)", self, item.type, item.title, path)
                        result.add(path)
        
        def process_server(server):
            result = set()
            for section in server.library.sections():
                if section.type not in {'movie', 'show'}:
                    continue
                
                if self.libraries and section.title not in self.libraries:
                    continue
                
                for item in section.search(unwatched=True):            
                    if item.type == 'movie':
                        __populate(item, result)
                    elif item.type == 'show':
                        for episode in item.episodes():
                            __populate(episode, result)
            return result
            
        results = asyncio.gather(*(asyncio.to_thread(process_server, server) for server in self.__plex_servers))
        
        async def process():
            not_watched = set()
            for result in await results:
                not_watched.update(result)
            
            logging.info("[%s] Found %d not-watched files in the plex library", self, len(not_watched))
            return not_watched
                                    
        return asyncio.create_task(process())
    
    async def is_active(self, file: str) -> bool:
        async def check_rating_key(ratingKey):
            item = await asyncio.to_thread(self.__plex.library.fetchItem(ratingKey))
            for media in item.media:
                for part in media.parts:
                    if not part.file:
                        continue
                    path = self.rewriter.on_source(part.file)
                    if os.path.exists(path) and os.path.samefile(path, file):
                        return True
            return False

        tasks = [asyncio.create_task(check_rating_key(rk)) for rk in self.__active_items()]
        
        result = False
        for coro in asyncio.as_completed(tasks):
            if await coro:
                for t in tasks:
                    if not t.done():
                        t.cancel()
                result = True
                break
                
        await asyncio.gather(*tasks, return_exceptions=True)

        return result
    
    def __active_items(self) -> Set[str]:
        return set([session.ratingKey for session in self.__plex.sessions()])
    
    @cached_property
    def __continue_watching_on_source(self) -> asyncio.Task[Set[str]]:
        async def process():
            return {
                self.rewriter.on_source(path)
                for bucket in await self.__continue_watching
                for media in bucket
                for path in media
                if os.path.exists(self.rewriter.on_source(path))
            }
        
        return asyncio.create_task(process())
        
    async def get_sort_key(self, path: str) -> int:
        not_watched = await self.not_watched_media
        continue_watching = await self.__continue_watching_on_source
        
        return (1 if path in not_watched else 0) + (1 if path in continue_watching else 0)
    
    async def continue_watching(self) -> List[str]:
        result: List[str] = []
        max_count: int = 25
        
        for bucket in await self.__continue_watching:
            remaining = max_count
            for item in bucket:
                if not remaining:
                    break
                
                remaining -= 1
                
                for path in item:
                    source_path = self.rewriter.on_source(path)
                    if source_path in await self.__continue_watching_on_source:
                        continue
                    
                    detination_path = self.rewriter.on_destination(path)
                    if not os.path.exists(detination_path):
                        continue
                    result.append(detination_path)
                
        logging.info("[%s] Detected %d watching files not currently available on source drives in Plex library", self, len(result))
        
        return result
    
    @cached_property
    def __continue_watching(self) -> asyncio.Task[List[List[Set[str]]]]:
        cutoff = self.now - timedelta(weeks=1)
        active_items = self.__active_items()
        pq = PriorityQueue()
        
        def __populate_watching(item):
            return {
                part.file
                for media in item.media
                for part in media.parts
            }

        async def get_continue_watching(server):
            def should_skip(item):
                return item.isWatched or item.ratingKey in active_items
                
            continue_watching = await asyncio.to_thread(server.continueWatching)
            for item in sorted(continue_watching, key=lambda i: i.lastViewedAt or 0, reverse=True):
                if self.libraries and item.librarySectionTitle not in self.libraries:
                    logging.debug("[%s] Item: %s is in %s library skipping...", self, item.title, item.librarySectionTitle)
                    continue
                
                if not item.lastViewedAt or item.lastViewedAt < cutoff:
                    logging.debug("[%s] Item: %s last watched at %s (cutoff: %s) — skipping...", self, item.title, item.lastViewedAt or "?", cutoff)
                    continue
                
                if item.type == 'movie':
                    if not should_skip(item):
                        await pq.put((-item.lastViewedAt.timestamp(), [__populate_watching(item)]))
                elif item.type == 'episode':
                    show = item.show()
                    key = (item.seasonNumber, item.index + 1) if should_skip(item) else (item.seasonNumber, item.index)
                    temp = []
                    for episode in sorted([e for e in show.episodes() if (e.seasonNumber, e.index) >= key], key=lambda e: (e.seasonNumber, e.index)):
                        temp.append(__populate_watching(episode))
                    await pq.put((-item.lastViewedAt.timestamp(), temp))
                
        results = asyncio.gather(*(get_continue_watching(server) for server in self.__plex_servers))
        
        async def process() -> List[List[Set[str]]]:
            await results
            
            result: List[List[Set[str]]] = []
            processed: Set[str] = set()
            while not pq.empty():
                _, media_list = await pq.get()
                temp: List[Set[str]] = []
                for media in media_list:
                    m: Set[str] = set()
                    for path in media:
                        if path in processed:
                            continue
                        processed.add(path)
                        m.add(path)
                    temp.append(m)
                
                result.append(temp)
    
            logging.info("[%s] Detected %d watching files in Plex library", self, len(result))
            return result
                    
        return asyncio.create_task(process())
    
    def __str__(self):
        return self.url
    
    def __repr__(self):
        return self.__str__()
    
    def type(self):
        return MediaPlayerType.PLEX