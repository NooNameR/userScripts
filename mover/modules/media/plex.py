import sys
import os
import logging
from .media_player import MediaPlayer, MediaPlayerType
from ..rewriter import Rewriter
from typing import Set, List
from queue import PriorityQueue
from datetime import datetime, timedelta
from functools import cached_property
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    def not_watched_media(self) -> Set[str]:
        with ThreadPoolExecutor(max_workers=3) as executor:
            def __populate(item, result):
                for media in item.media:
                    for part in media.parts:
                        path = self.rewriter.on_source(part.file)
                        if os.path.exists(path):
                            logging.debug("Non-Watched %s: %s (%s)", item.type, item.title, path)
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
            
            not_watched = set()
            futures = [executor.submit(process_server, server) for server in self.__plex_servers]
            for future in as_completed(futures):
                not_watched.update(future.result())
            
        logging.info("Found %d not-watched files in the plex library", len(not_watched))
                                    
        return not_watched
    
    def is_active(self, file: str) -> bool:
        for ratingKey in self.__active_items():
            item = self.__plex.library.fetchItem(ratingKey)
            for media in item.media:
                for part in media.parts:
                    if not part.file:
                        continue

                    path = self.rewriter.on_source(part.file)
                    if os.path.exists(path) and os.path.samefile(path, file):
                        return True
        
        return False
    
    def __active_items(self) -> Set[str]:
        return set([session.ratingKey for session in self.__plex.sessions()])
    
    @cached_property
    def __continue_watching_on_source(self) -> Set[str]:
        return {
            self.rewriter.on_source(path)
            for bucket in self.__continue_watching
            for media in bucket
            for path in media
            if os.path.exists(self.rewriter.on_source(path))
        }
        
    def get_sort_key(self, path: str) -> int:
        return (1 if path in self.not_watched_media else 0) + (1 if path in self.__continue_watching_on_source else 0)
    
    def continue_watching(self) -> List[str]:
        result: List[str] = []
        max_count: int = 25
        
        for bucket in self.__continue_watching:
            remaining = max_count
            for item in bucket:
                if not remaining:
                    break
                
                remaining -= 1
                
                for path in item:
                    source_path = self.rewriter.on_source(path)
                    if source_path in self.__continue_watching_on_source:
                        continue
                    
                    detination_path = self.rewriter.on_destination(path)
                    if not os.path.exists(detination_path):
                        continue
                    result.append(detination_path)
                
        logging.info("Detected %d watching files not currently available on source drives in Plex library", len(result))
        
        return result
    
    @cached_property
    def __continue_watching(self) -> List[List[Set[str]]]:
        cutoff = self.now - timedelta(weeks=1)
        active_items = self.__active_items()
        pq = PriorityQueue()
        
        def __populate_watching(item):
            return {
                part.file
                for media in item.media
                for part in media.parts
            }

        def get_continue_watching(server):
            def should_skip(item):
                return item.isWatched or item.ratingKey in active_items
                
            for item in sorted(server.continueWatching(), key=lambda i: i.lastViewedAt or 0, reverse=True):
                if self.libraries and item.librarySectionTitle not in self.libraries:
                    logging.debug("Item: %s is in %s library skipping...", item.title, item.librarySectionTitle)
                    continue
                
                if not item.lastViewedAt or item.lastViewedAt < cutoff:
                    logging.debug("Item: %s last watched at %s (cutoff: %s) — skipping...", item.title, item.lastViewedAt or "?", cutoff)
                    continue
                
                if item.type == 'movie':
                    if not should_skip(item):
                        pq.put((-item.lastViewedAt.timestamp(), [__populate_watching(item)]))
                elif item.type == 'episode':
                    show = item.show()
                    key = (item.seasonNumber, item.index + 1) if should_skip(item) else (item.seasonNumber, item.index)
                    temp = []
                    for episode in sorted([e for e in show.episodes() if (e.seasonNumber, e.index) >= key], key=lambda e: (e.seasonNumber, e.index)):
                        temp.append(__populate_watching(episode))
                    pq.put((-item.lastViewedAt.timestamp(), temp))
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            for future in [executor.submit(get_continue_watching, server) for server in self.__plex_servers]:
                future.result()
            
        result: List[List[Set[str]]] = []
        processed: Set[str] = set()
        while not pq.empty():
            _, media_list = pq.get()
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
    
        logging.info("Detected %d watching files in Plex library", len(result))
                    
        return result
    
    def __str__(self):
        return self.url
    
    def __repr__(self):
        return self.__str__()
    
    def type(self):
        return MediaPlayerType.PLEX