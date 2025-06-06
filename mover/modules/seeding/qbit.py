import os
import sys
import asyncio
import logging
from functools import cached_property
from ..rewriter import Rewriter
from .seeding_client import SeedingClient
from typing import Tuple, Set
from collections import defaultdict
from ..helpers import execute, get_stat
from datetime import datetime

class Qbit(SeedingClient):
    def __init__(self, now: datetime, rewriter: Rewriter, host: str, user: str, password: str):
        self.now = now.timestamp()
        self.rewriter: Rewriter = rewriter
        self.host: str = host
        self.user: str = user
        self.password: str = password
        self.paused_torrents = []
        self.seen: Set[str] = set()
        self.cache = defaultdict(list)
        self.sem: asyncio.Semaphore = asyncio.Semaphore(1)
        self.logger = logging.getLogger(__name__)
        
    @cached_property
    def __client(self):
        try:
            from qbittorrentapi import APIConnectionError
            from qbittorrentapi import Client
            from qbittorrentapi import LoginFailed
        except ModuleNotFoundError:
            self.logger.error('Requirements Error: qbittorrent-api not installed. Please install using the command "pip install qbittorrent-api"')
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
        return self.__client.torrents.info(status_filter="completed", sort="completion_on", reverse=True)
    
    async def __get_torrents(self, inode: int):
        async with self.sem:
            return list(self.cache[inode])
        
    async def scan(self, root: str) -> None:
        if root in self.seen:
            return
        
        self.logger.info("[%s] Scanning torrents on %s...", self, root)
        
        await self.sem.acquire()
        
        def submit():
            total = 0
            for torrent in self.__torrents:
                path = self.rewriter.rewrite(root, torrent.content_path)
                if not os.path.exists(path):
                    continue
                if os.path.isdir(path):
                    for root_, _, files in os.walk(path):
                        for file in files:
                            full_path = os.path.join(root_, file)
                            self.cache[get_stat(full_path).st_ino].append(torrent)
                else:
                    self.cache[get_stat(path).st_ino].append(torrent)
                    
                total += 1
        
            self.logger.info("[%s] Found %d torrents on %s", self, total, root)
        
        async def wrapper():
            try:
                await asyncio.to_thread(submit)
                self.seen.add(root)
            finally:
                self.sem.release()
        
        asyncio.create_task(wrapper())

    async def get_sort_key(self, path: str) -> Set[Tuple[float, int, int]]:
        inode = get_stat(path).st_ino
        torrents = await self.__get_torrents(inode)
        return {(torrent.eta or 0, self.now - torrent.completion_on, torrent.num_seeds) for torrent in torrents}
        
    async def pause(self, path: str) -> None:
        inode = get_stat(path).st_ino
        torrents = await self.__get_torrents(inode)
        for torrent in torrents:
            if torrent in self.paused_torrents:
                continue
            
            self.logger.info("[%s] [%s] Pausing torrent: %s [%d] -> %s", self, torrent.hash, torrent.name, torrent.added_on, torrent.content_path)
            execute(torrent.pause)
            self.paused_torrents.append(torrent)
        
    async def resume(self) -> None:
        while self.paused_torrents:
            torrent = self.paused_torrents.pop()
            self.logger.info("[%s] [%s] Resuming torrent: %s [%d] -> %s", self, torrent.hash, torrent.name, torrent.added_on, torrent.content_path)
            execute(torrent.resume)
            
    async def aclose(self) -> None:
        await self.resume()

    def __str__(self):
        return f"qbittorrent@{self.host}"

    def __repr__(self):
        return self.__str__()