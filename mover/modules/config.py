import pyaml_env
import fnmatch
import os
import shutil
import logging
import asyncio
from .media.plex import Plex
from .media.media_player import MediaPlayer
from .seeding.qbit import Qbit
from .seeding.seeding_client import SeedingClient
from .helpers import get_ctime, get_stat
from datetime import datetime, timedelta
from typing import Dict, Tuple, Set, List
from .rewriter import Rewriter, RealRewriter, NoopRewriter
from pytimeparse2 import parse

class Config:
    def __init__(self, now: datetime, path='config.yaml'):
        self.now: datetime = now
        self.raw = pyaml_env.parse_config(path)
    
        self.mappings: List["MovingMapping"] = [self.__parse_mapping(m) for m in self.raw.get("mappings", [])]
        
    def __parse_mapping(self, m) -> "MovingMapping":
        return MovingMapping(self.now, m)
    
    def __str__(self) -> str:
        out = [
            f"Config:",
            f"  Mappings:"
        ]
        for i, mapping in enumerate(self.mappings, 1):
            out.append(f"    {i}. {mapping}")
        return "\n".join(out)

class MovingMapping:   
    def __init__(self, now: datetime, raw):
        self.now: datetime = now
        self.source: str = raw["source"]
        self.destination: str = raw["destination"]
        self.threshold: float = raw.get("threshold", 0.0)
        self.cache_threshold: float = raw.get("cache_threshold", 0.0)
        self.min_age: int = parse(raw.get("min_age", "2h"))
        self.max_age: int = parse(raw.get("max_age")) if raw.get("max_age") else float('inf')
        self.clients: List[SeedingClient] = [Qbit(now=self.now, rewriter=self.__parse_rewriter(self.source, self.destination, client.pop("rewrite", {})), **client) for client in raw.get("clients", [])]
        self.plex: List[MediaPlayer] = [Plex(now=self.now, rewriter=self.__parse_rewriter(self.source, self.destination, client.pop("rewrite", {})), **client) for client in raw.get("plex", [])]
        self.ignores: Set[str] = set(raw.get("ignore", []))
        
    def __parse_rewriter(self, source: str, destination: str, rewrite: Dict[str, str] = {}) -> Rewriter:
        if rewrite and "from" in rewrite and "to" in rewrite:
            src, dst = rewrite["from"], rewrite["to"]
            return RealRewriter(source, destination, src, dst)
        else:
            return NoopRewriter(source, destination)
        
    async def needs_moving(self) -> int:
        total, used, _ = shutil.disk_usage(self.source)
        percent_used = round((used / total) * 100, 4)
        threshold_bytes = used - int(total * (self.threshold / 100))
        
        if threshold_bytes > 0:
            await asyncio.gather(*(client.scan(self.source) for client in self.clients))
            
            logging.debug("Space usage: %.4g%% is above moving threshold: %.4g%%. Starting %s...", percent_used, self.threshold, self.source)
            return threshold_bytes
        
        logging.info("Space usage: %.4g%% is below moving threshold: %.4g%%. Skipping %s...", percent_used, self.threshold, self.source)
        return 0

    async def can_move_to_source(self) -> int:
        if not self.cache_threshold:
            return 0
        
        total, used, _ = shutil.disk_usage(self.source)
        percent_used = round((used / total) * 100, 4)
        threshold_bytes = int(total * (self.cache_threshold / 100)) - used
        
        if threshold_bytes > 0:
            results = await asyncio.gather(*(plex.continue_watching for plex in self.plex))
            
            if all(not r for r in results):
                logging.info("No continue watching items from any Plex client. Skipping %s...", self.source)
                return 0
            
            await asyncio.gather(*(client.scan(self.destination) for client in self.clients))
                
            logging.debug("Space usage: %.4g%% is below cache threshold: %.4g%%. Starting %s...", percent_used, self.cache_threshold, self.source)
            return threshold_bytes
        
        logging.info("Space usage: %.4g%% is above cache threshold: %.4g%%. Skipping %s...", percent_used, self.cache_threshold, self.source)
        return 0
    
    async def eligible_for_source(self) -> List[str]:
        results = await asyncio.gather(*(plex.continue_watching for plex in self.plex))
        return [i for plex in results for i in plex]
    
    def get_src_file(self, path: str) -> str:
        rel_path = os.path.relpath(path, self.destination)
        return os.path.join(self.source, rel_path)
        
    def get_dest_file(self, src_path: str) -> str:
        rel_path = os.path.relpath(src_path, self.source)
        return os.path.join(self.destination, rel_path)
        
    def pause(self, path: str) -> asyncio.Future:
        return asyncio.gather(*(qbit.pause(path) for qbit in self.clients))
            
    def resume(self) -> asyncio.Future:
        return asyncio.gather(*(qbit.resume() for qbit in self.clients))
            
    async def get_sort_key(self, path: str) -> Tuple[int, int, int, int, int, int, float]:
        # ignored path, no point checking
        if self.is_ignored(path):
            return (1, 0, 0, 0, 0, 0, 0)
        
        def within_range(age: float):
            return self.min_age <= age <= self.max_age
        
        ctime = get_ctime(path)
        age_priority = 0 if within_range(self.now.timestamp() - ctime) else 1
        
        qbit_results: List[Tuple[int, int]]
        plex_results: List[int]
    
        qbit_results, plex_results = await asyncio.gather(
            asyncio.gather(*(qbit.get_sort_key(path) for qbit in self.clients)),
            asyncio.gather(*(plex.get_sort_key(path) for plex in self.plex))
        )

        completion_age, num_seeders = min({(min(a for a, _ in res), min(n for _, n in res)) for res in qbit_results if res}, default=(0, 0))
        plex_key = max(plex_results, default = 0)
        has_torrent = 1 if qbit_results else 0
        size = get_stat(path).st_size
        
        return (
            age_priority,       # 1. age_priority (0 if within age range, else 1)
            plex_key,           # 2. plex un-watched -> 1, watched 0
            has_torrent,        # 3. has_torrent (0 if has torrents, else 1)
            -completion_age,    # 4. -completion_age (negative to prioritize older completion age)
            -num_seeders,       # 5. -num_seeders (negative to prioritize more seeders)
            len(qbit_results),  # 6. num seeding torrents
            -size,              # 7. bigger file goes first
            ctime               # 8. ctime (file creation time as tiebreaker)
        )
    
    async def is_active(self, file: str) -> bool:
        tasks = [asyncio.create_task(plex.is_active(file)) for plex in self.plex]
        
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
        
    def is_ignored(self, path: str) -> bool:
        if not self.ignores:
            return False
        
        return any(fnmatch.fnmatch(path, pattern) for pattern in self.ignores)
    
    def __str__(self) -> str:
        return (
            f"Mapping:\n"
            f"       Source: {self.source}\n"
            f"       Destination: {self.destination}\n"
            f"       Threshold: {self.threshold:.4g}%\n"
            f"       Cache Threshold: {self.cache_threshold:.4g}%\n"
            f"       Age range: {timedelta(seconds=self.min_age)} – {"..." if self.max_age == float('inf') else timedelta(seconds=self.max_age)}\n"
            f"       Clients: [{', '.join(map(str, self.clients))}]\n"
            f"       Plex: [{', '.join(map(str, self.plex))}]\n"
            f"       Ignore patterns: [{', '.join(map(str, self.ignores))}]"
        )