import sys
import os
import logging
from typing import Dict
from collections import OrderedDict
from datetime import datetime, timedelta
from functools import cached_property

class PlexHelper:
    def __init__(self, url: str, token: str, rewrite: Dict[str, str] = {}):
        self.url = url
        self.token = token
        if rewrite and "from" in rewrite and "to" in rewrite:
            src, dst = rewrite["from"], rewrite["to"]
            self.rewriter = lambda path: path.replace(src, dst, 1)
        else:
            self.rewriter = lambda path: path  # no-op
        self.media = set()
        
    @cached_property
    def __plex(self):
        try:
            from plexapi.server import PlexServer
        except ModuleNotFoundError:
            logging.error('Requirements Error: plexapi not installed. Please install using the command "pip install plexapi"')
            sys.exit(1)
            
        return PlexServer(self.url, self.token)
    
    @cached_property
    def watched_media(self):
        watched = set()
        plex = self.__plex
        
        def __populate_watched(item):
            for media in item.media:
                for part in media.parts:
                    path = self.rewriter(part.file)
                    if os.path.exists(path):
                        logging.debug("Watched %s: %s (%s)", item.type, item.title, path)
                        watched.add(path)
            
        for section in plex.library.sections():
            if section.type not in {'movie', 'show'}:
                continue
            
            for item in section.search(unwatched=False):            
                if item.type == 'movie':
                    __populate_watched(item)
                elif item.type == 'show':
                    for episode in item.episodes():
                       __populate_watched(episode)
                       
        logging.info("Found %d watched files in the plex library", len(watched))
                                    
        return watched
    
    def is_active(self, file: str) -> bool:
        for ratingKey in self.__active_items():
            item = self.__plex.library.fetchItem(ratingKey)
            for media in item.media:
                for part in media.parts:
                    if not part.file:
                        continue

                    path = self.rewriter(part.file)
                    if os.path.exists(path) and os.path.samefile(path, file):
                        return True
        
        return False
    
    def __active_items(self) -> set[str]:
        result = set()
        for session in self.__plex.sessions():
            result.add(session.ratingKey)
        
        return result

    def is_watched(self, file: str) -> bool:
        return file in self.watched_media
    
    @cached_property
    def continue_watching(self) -> set[str]:
        result = OrderedDict()
        cutoff = datetime.now() - timedelta(weeks=2)
        
        def __populate_watching(item):
            for media in item.media:
                for part in media.parts:
                    path = self.rewriter(part.file)
                    if not os.path.exists(path):
                        logging.debug("Watching not on source %s: %s (%s)", item.type, item.title, path)
                        result[path] = None
        
        active_items = self.__active_items()
        for item in sorted(self.__plex.continueWatching(), key=lambda i: i.lastViewedAt or 0, reverse=True):
            if not item.lastViewedAt or item.lastViewedAt < cutoff:
                logging.info("Item: %s last watched at %s (cutoff: %s) — skipping...", item.title, item.lastViewedAt or "?", cutoff)
                continue
            
            if item.type == 'movie':
                __populate_watching(item)
            elif item.type == 'episode':
                show = item.show()
                key = (item.seasonNumber, item.index) if item.isWatched and item.ratingKey in active_items else (item.seasonNumber, item.index -1)
                for episode in sorted([e for e in show.episodes() if (e.seasonNumber, e.index) > key], key=lambda e: (e.seasonNumber, e.index)):                    
                    if episode.seasonNumber < item.seasonNumber and episode.index < item.index:
                        continue
                    
                    __populate_watching(episode)
        
        logging.info("Detected %d watching files not currently available on source drives in Plex library", len(result))
                    
        return list(result.keys())
    
    def __str__(self):
        return self.url
    
    def __repr__(self):
        return self.__str__()