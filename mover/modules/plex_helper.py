import sys
import os
import logging
from functools import cached_property

class PlexHelper:
    def __init__(self, url, token, rewrite = {}, users: set[str] = set()):
        self.url = url
        self.token = token
        self.users = users
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
            
        for section in self.__plex.library.sections():
            if section.type not in {'movie', 'show'}:
                continue
            
            for item in section.search(unwatched=False):
                if section.type == 'movie':
                    for media in item.media:
                        for mediapart in media.parts:
                            path = self.rewriter(mediapart.file)
                            if os.path.exists(path):
                                logging.debug("Marking movie: %s (%s) as watched", item.title, path)
                                watched.add(path)
                elif section.type == 'show':
                    for episode in item.episodes():
                        for media in episode.media:
                            for mediapart in media.parts:
                                path = self.rewriter(mediapart.file)
                                if os.path.exists(path):
                                    logging.debug("Marking episode: %s (%s) as watched", episode.title, path)
                                    watched.add(path)
                                    
        return watched
                        
    def is_watched(self, file: str) -> bool:
        return file in self.watched_media
    
    def __str__(self):
        return self.url