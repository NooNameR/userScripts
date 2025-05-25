import os
import sys
import logging
from functools import cached_property
from typing import Dict
from .helpers import execute

class QbitHelper:
    def __init__(self, host: str, user: str, password: str, rewrite: Dict[str, str] = {}):
        self.rewrite = rewrite
        self.host = host
        self.user = user
        if rewrite and "from" in rewrite and "to" in rewrite:
            src, dst = rewrite["from"], rewrite["to"]
            self.rewriter = lambda path: path.replace(src, dst, 1)
        else:
            self.rewriter = lambda path: path  # no-op
        self.password = password
        self.torrents = []
        self.paused_torrents = []
        
    @cached_property
    def __client(self):
        try:
            from qbittorrentapi import APIConnectionError
            from qbittorrentapi import Client
            from qbittorrentapi import LoginFailed
        except ModuleNotFoundError:
            logging.error('Requirements Error: qbittorrent-api not installed. Please install using the command "pip install qbittorrent-api"')
            sys.exit(1)
            
        try:
            return Client(host=self.host, username=self.user, password=self.password)
        except LoginFailed:
            raise ("Qbittorrent Error: Failed to login. Invalid username/password.")
        except APIConnectionError:
            raise ("Qbittorrent Error: Unable to connect to the client.")
        except Exception:
            raise ("Qbittorrent Error: Unable to connect to the client.")
        
    def __torrents(self):
        if self.torrents:
            return self.torrents
        
        self.torrents = self.__filter(self.__client.torrents.info(status_filter="completed", sort="added_on", reverse=True))
        logging.debug(f"Found %d torrents", len(self.torrents))
        return self.torrents
    
    def __cache_path(self, torrent) -> str:
        return self.rewriter(torrent.content_path)
            
    def __filter(self, torrents):
        result = []
        for torrent in torrents:
            if os.path.exists(self.__cache_path(torrent)):
                result.append(torrent)
        return result
            
    def __has_file(self, torrent, path: str) -> bool:
        content_path = self.__cache_path(torrent)
        
        if not os.path.exists(content_path):
            return False
        
        if os.path.isdir(content_path):
            for root, _, files in os.walk(content_path):
                for file in files:
                    full_path = os.path.join(root, file)
                    if os.path.samefile(full_path, path):
                        return True
            return False
    
        return os.path.samefile(content_path, path)
        
    def pause(self, path: str):
        for torrent in self.__torrents():
           if self.__has_file(torrent, path):
               logging.info("[%s] Pausing torrent: %s [%d] -> %s", torrent.hash, torrent.name, torrent.added_on, torrent.content_path)
               execute(torrent.pause)
               self.paused_torrents.append(torrent)
        
        self.torrents = [t for t in self.torrents if t not in self.paused_torrents]
        
    def resume(self):
        for torrent in self.paused_torrents:
            logging.info("[%s] Resuming torrent: %s [%d] -> %s", torrent.hash, torrent.name, torrent.added_on, torrent.content_path)
            execute(torrent.resume)

    def __str__(self):
        return self.host

    def __repr__(self):
        return self.__str__()