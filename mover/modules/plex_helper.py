import sys
import os
import logging
from .rewriter import Rewriter, RealRewriter, NoopRewriter
from typing import Dict
from collections import OrderedDict
from datetime import datetime, timedelta
from functools import cached_property

class PlexHelper:
    def __init__(self, source: str, url: str, token: str, libraries: list[str] = [], users: list[str] = [], rewrite: Dict[str, str] = {}):
        self.url = url
        self.token = token
        self.libraries = set(libraries)
        self.users = set(users)
        if rewrite and "from" in rewrite and "to" in rewrite:
            src, dst = rewrite["from"], rewrite["to"]
            self.rewriter: Rewriter = RealRewriter(source, src, dst)
        else:
            self.rewriter: Rewriter = NoopRewriter()
        
    @cached_property
    def __plex(self):
        return self.get_plex_server(self.token)
    
    @cached_property
    def __plex_servers(self):
        plex = self.__plex
        
        return [plex] + [plex.switchUser(u) for u in plex.myPlexAccount().users() if not self.users or u.username in self.users]
    
    def get_plex_server(self, token):
        try:
            from plexapi.server import PlexServer
        except ModuleNotFoundError:
            logging.error('Requirements Error: plexapi not installed. Please install using the command "pip install plexapi"')
            sys.exit(1)
            
        return PlexServer(self.url, token)
    
    @cached_property
    def not_watched_media(self) -> set[str]:
        not_watched = set()
        
        def __populate_watched(item):
            for media in item.media:
                for part in media.parts:
                    path = self.rewriter.rewrite(part.file)
                    if os.path.exists(path):
                        logging.debug("Watched %s: %s (%s)", item.type, item.title, path)
                        not_watched.add(path)
        
        for plex in self.__plex_servers:
            for section in plex.library.sections():
                if section.type not in {'movie', 'show'}:
                    continue
                
                if self.libraries and section.title not in self.libraries:
                    continue
                
                for item in section.search(unwatched=True):            
                    if item.type == 'movie':
                        __populate_watched(item)
                    elif item.type == 'show':
                        for episode in item.episodes():
                            __populate_watched(episode)
                       
        logging.info("Found %d not-watched files in the plex library", len(not_watched))
                                    
        return not_watched
    
    def is_active(self, file: str) -> bool:
        for ratingKey in self.__active_items():
            item = self.__plex.library.fetchItem(ratingKey)
            for media in item.media:
                for part in media.parts:
                    if not part.file:
                        continue

                    path = self.rewriter.rewrite(part.file)
                    if os.path.exists(path) and os.path.samefile(path, file):
                        return True
        
        return False
    
    def __active_items(self) -> set[str]:
        return set([session.ratingKey for session in self.__plex.sessions()])

    def is_not_watched(self, file: str) -> bool:
        return file in self.not_watched_media
    
    @cached_property
    def continue_watching(self) -> list[str]:
        result = OrderedDict()
        cutoff = datetime.now() - timedelta(weeks=1)
        active_items = self.__active_items()
        
        def __populate_watching(item):
            for media in item.media:
                for part in media.parts:
                    path = self.rewriter.rewrite(part.file)
                    if not os.path.exists(path):
                        logging.debug("Watching not on source %s: %s (%s)", item.type, item.title, path)
                        result[path] = None
                        
        def get_continue_watching(items):
            def should_skip(item):
                return item.isWatched or item.ratingKey in active_items
                
            for item in sorted(items, key=lambda i: i.lastViewedAt or 0, reverse=True):
                if self.libraries and item.librarySectionTitle not in self.libraries:
                    logging.debug("Item: %s is in %s library skipping...", item.title, item.librarySectionTitle)
                    continue
                
                if not item.lastViewedAt or item.lastViewedAt < cutoff:
                    logging.debug("Item: %s last watched at %s (cutoff: %s) — skipping...", item.title, item.lastViewedAt or "?", cutoff)
                    continue
                
                if item.type == 'movie':
                    if not should_skip(item):
                        __populate_watching(item)
                elif item.type == 'episode':
                    remaining = 25
                    show = item.show()
                    key = (item.seasonNumber, item.index + 1) if should_skip(item) else (item.seasonNumber, item.index)
                    for episode in sorted([e for e in show.episodes() if (e.seasonNumber, e.index) >= key], key=lambda e: (e.seasonNumber, e.index)):
                        if not remaining:
                            break
                        
                        __populate_watching(episode)
                        remaining -= 1

        for server in self.__plex_servers:
            get_continue_watching(server.continueWatching())
    
        logging.info("Detected %d watching files not currently available on source drives in Plex library", len(result))
                    
        return list(result.keys())
    
    def __str__(self):
        return self.url
    
    def __repr__(self):
        return self.__str__()