import os
import sys
import logging
from functools import cached_property
from ..rewriter import Rewriter
from .seeding_client import SeedingClient
from typing import Tuple, Set
from collections import defaultdict
from ..helpers import execute
from datetime import datetime

class Qbit(SeedingClient):
    def __init__(self, now: datetime, rewriter: Rewriter, host: str, user: str, password: str):
        self.now = now.timestamp()
        self.rewriter: Rewriter = rewriter
        self.host: str = host
        self.user: str = user
        self.password: str = password
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
    
    @cached_property
    def __torrents(self):
        torrents = self.__client.torrents.info(status_filter="completed", sort="completion_on", reverse=True)
        
        result = defaultdict(list)
        total = 0
        for torrent in torrents:
            path = self.rewriter.on_source(torrent.content_path)
            if os.path.exists(path):
                total += 1
                if os.path.isdir(path):
                    for root, _, files in os.walk(path):
                        for file in files:
                            full_path = os.path.join(root, file)
                            result[full_path].append(torrent)
                else:
                    result[path].append(torrent)
                    
        logging.info(f"Found %d torrents on source", total)

        return result
    
    def get_sort_key(self, path: str) -> Set[Tuple[int, int]]:
        return {(self.now - torrent.completion_on, torrent.num_seeds) for torrent in self.__torrents[path]}
        
    def pause(self, path: str):
        for torrent in self.__torrents[path]:
            if torrent in self.paused_torrents:
                continue
            
            logging.info("[%s] Pausing torrent: %s [%d] -> %s", torrent.hash, torrent.name, torrent.added_on, torrent.content_path)
            execute(torrent.pause)
            self.paused_torrents.append(torrent)
        
    def resume(self):
        while self.paused_torrents:
            torrent = self.paused_torrents.pop()
            logging.info("[%s] Resuming torrent: %s [%d] -> %s", torrent.hash, torrent.name, torrent.added_on, torrent.content_path)
            execute(torrent.resume)

    def __str__(self):
        return self.host

    def __repr__(self):
        return self.__str__()