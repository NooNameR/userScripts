import sys
import os
import logging
import asyncio
import threading
from .media_player import MediaPlayer, MediaPlayerType
from ..rewriter import Rewriter
from collections import defaultdict
from typing import Set, List, Tuple, Dict
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
        self._lock = threading.Lock()
        self.logger = logging.getLogger(__name__)
        
    @cached_property
    def __plex(self):
        with self._lock:
            return self.get_plex_server(self.token)
    
    @cached_property
    def __plex_servers(self):
        plex = self.__plex
        
        return [plex] + [plex.switchUser(u) for u in plex.myPlexAccount().users() if not self.users or u.username in self.users]
    
    def get_plex_server(self, token: str):
        try:
            from plexapi.server import PlexServer
        except ModuleNotFoundError:
            self.logger.error('Requirements Error: plexapi not installed. Please install using the command "pip install plexapi"')
            sys.exit(1)
            
        return PlexServer(self.url, token)
    
    @cached_property
    def media(self) -> asyncio.Task[Set[str]]:        
        async def process_server(server):      
            def process_section(section):
                local_state: Set[str] = set()
                
                def __populate(item):
                    for media in item.media:
                        for part in media.parts:
                            path = self.rewriter.on_source(part.file)
                            if os.path.exists(path):
                                self.logger.debug("[%s] Processing %s: %s (%s)", self, item.type, item.title, path)
                                local_state.add(path)
                                
                for item in section.search(unwatched=True):
                    if item.type == 'movie':
                        __populate(item)
                    elif item.type == 'show':
                        for episode in item.episodes():
                            __populate(episode)

                return local_state
            
            local_states: Set[str] = await asyncio.gather(*(
                asyncio.to_thread(process_section, section)
                for section in server.library.sections() 
                if section.type in {'movie', 'show'}
                if not self.libraries or (section.title in self.libraries)
            ))
            
            return {p for lib in local_states for p in lib}
        
        async def process():
            user_results = [process_server(server) for server in self.__plex_servers]
            
            un_watched_counts: Dict[str, int] = defaultdict(int)
            
            for user_result in asyncio.as_completed(user_results):
                for path in await user_result:
                    un_watched_counts[path] += 1
            
            self.logger.info("[%s] Found %d not-watched files in the Plex library", self, len(un_watched_counts))
            return un_watched_counts
                                    
        return asyncio.create_task(process())
    
    async def is_active(self, file: str) -> bool:
        async def check_rating_key(ratingKey):
            item = await asyncio.to_thread(self.__plex.library.fetchItem, ratingKey)
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
        return {session.ratingKey for session in self.__plex.sessions()}
    
    @cached_property
    def __continue_watching_on_source(self) -> asyncio.Task[Set[str]]:
        async def process():
            return {
                self.rewriter.on_source(path)
                for _, bucket in await self.__continue_watching
                for media in bucket
                for path in media
                if os.path.exists(self.rewriter.on_source(path))
            }
        
        return asyncio.create_task(process())
        
    async def get_sort_key(self, path: str) -> Tuple[bool, int]:
        un_watched, continue_watching = await asyncio.gather(
            self.media,
            self.__continue_watching_on_source
        )
        
        return (path in continue_watching, un_watched.get(path, 0))
    
    async def continue_watching(self, pq: asyncio.Queue[Tuple[float, int, str]]) -> None:
        max_count: int = 25
        total: int = 0
        
        for key, bucket in await self.__continue_watching:
            remaining = max_count
            for item in bucket:
                if not remaining:
                    break
                
                remaining -= 1
                
                for index, path in enumerate(item):
                    source_path = self.rewriter.on_source(path)
                    if source_path in await self.__continue_watching_on_source:
                        continue
                    
                    detination_path = self.rewriter.on_destination(path)
                    if not os.path.exists(detination_path):
                        continue
                    await pq.put((key, index, detination_path))
                    total += 1
                
        self.logger.info("[%s] Detected %d watching files not currently available on source drives in Plex library", self, total)
    
    @cached_property
    def __continue_watching(self) -> asyncio.Task[List[List[Set[str]]]]:
        cutoff = self.now - timedelta(weeks=1)
        pq = asyncio.PriorityQueue()
        
        def __populate_watching(item):
            return {
                part.file
                for media in item.media
                for part in media.parts
            }

        async def get_continue_watching(server):
            def should_skip(item):
                return item.isWatched
                
            continue_watching = await asyncio.to_thread(server.continueWatching)
            for item in sorted(continue_watching, key=lambda i: i.lastViewedAt or 0, reverse=True):
                if self.libraries and item.librarySectionTitle not in self.libraries:
                    self.logger.debug("[%s] Item: %s is in %s library skipping...", self, item.title, item.librarySectionTitle)
                    continue
                
                if not item.lastViewedAt or item.lastViewedAt < cutoff:
                    self.logger.debug("[%s] Item: %s last watched at %s (cutoff: %s) — skipping...", self, item.title, item.lastViewedAt or "?", cutoff)
                    continue
                
                if item.type == 'movie':
                    if not should_skip(item):
                        await pq.put((-item.lastViewedAt.timestamp(), [__populate_watching(item)]))
                elif item.type == 'episode':
                    show = item.show()
                    key = (item.seasonNumber, item.index + 1) if item.isWatched else (item.seasonNumber, item.index)
                    temp: List[Set[str]] = []
                    for episode in sorted([e for e in show.episodes() if (e.seasonNumber, e.index) >= key], key=lambda e: (e.seasonNumber, e.index)):
                        temp.append(__populate_watching(episode))
                    await pq.put((-item.lastViewedAt.timestamp(), temp))
        
        async def process() -> List[Tuple[float, List[Set[str]]]]:
            await asyncio.gather(*(get_continue_watching(server) for server in self.__plex_servers))
            
            result: List[List[Set[str]]] = []
            processed: Set[str] = set()
            while not pq.empty():
                key, media_list = await pq.get()
                temp: List[Set[str]] = []
                for media in media_list:
                    m: Set[str] = set()
                    for path in media:
                        if path in processed:
                            continue
                        processed.add(path)
                        m.add(path)
                    temp.append(m)
                
                result.append((key, temp))
            
            self.logger.info("[%s] Detected %d watching files in Plex library", self, len(result))
            return result
                    
        return asyncio.create_task(process())
    
    def __str__(self):
        return f"{self.type.name}@{self.url}".lower()
    
    def __repr__(self):
        return self.__str__()
    
    @property
    def type(self):
        return MediaPlayerType.PLEX
    
    async def aclose(self):
        pass