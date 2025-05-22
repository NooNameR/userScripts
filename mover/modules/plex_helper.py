import sys
import os
import logging
from functools import cached_property

class PlexHelper:
    def __init__(self, url, token, rewrite = {}):
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
            paths = set()
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
                elif section.type == 'show':
                    for episode in item.episodes():
                       __populate_watched(episode)
                       
        logging.info("Found %d watched items in the plex library", len(watched))
                                    
        return watched
    
    def is_active(self, file: str) -> bool:
        for session in self.__plex.sessions():
            item = self.__plex.library.fetchItem(session.ratingKey)
            for media in item.media:
                for part in media.parts:
                    if not part.file:
                        continue

                    path = self.rewriter(part.file)
                    if os.path.exists(path) and os.path.samefile(file):
                        return True
        
        return False

    def is_watched(self, file: str) -> bool:
        return file in self.watched_media
    
    def __str__(self):
        return self.url