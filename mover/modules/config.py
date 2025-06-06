import pyaml_env
import fnmatch
import os
import shutil
import logging
import asyncio
from .media.plex import Plex
from .media.jellyfin import Jellyfin
from .media.media_player import MediaPlayer
from .seeding.qbit import Qbit
from .seeding.seeding_client import SeedingClient
from .helpers import get_ctime, get_stat, format_bytes_to_gib, get_age_str
from datetime import datetime, timedelta
from typing import Dict, Tuple, Set, List
from functools import cached_property
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
        self.logger = logging.getLogger(__name__)
        self.now: datetime = now
        self.source: str = raw["source"]
        self.destination: str = raw["destination"]
        self.threshold: float = raw.get("threshold", 0.0)
        self.cache_threshold: float = raw.get("cache_threshold", 0.0)
        self.min_age: int = parse(raw.get("min_age", "2h"))
        self.max_age: int = parse(raw.get("max_age")) if raw.get("max_age") else float('inf')
        self.clients: List[SeedingClient] = [Qbit(now=self.now, rewriter=self.__parse_rewriter(self.source, self.destination, client.pop("rewrite", {})), **client) for client in raw.get("clients", [])]
        self.media: List[MediaPlayer] = [Plex(now=self.now, rewriter=self.__parse_rewriter(self.source, self.destination, client.pop("rewrite", {})), **client) for client in raw.get("plex", [])] + [Jellyfin(now=self.now, rewriter=self.__parse_rewriter(self.source, self.destination, client.pop("rewrite", {})), **client) for client in raw.get("jellyfin", [])]
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
            
            self.logger.debug("Space usage: %.4g%% is above moving threshold: %.4g%%. Starting %s...", percent_used, self.threshold, self.source)
            return threshold_bytes
        
        self.logger.info("Space usage: %.4g%% is below moving threshold: %.4g%%. Skipping %s...", percent_used, self.threshold, self.source)
        return 0

    async def can_move_to_source(self) -> int:
        if not self.cache_threshold:
            return 0
        
        total, used, _ = shutil.disk_usage(self.source)
        percent_used = round((used / total) * 100, 4)
        threshold_bytes = int(total * (self.cache_threshold / 100)) - used
        
        if threshold_bytes > 0:
            results = await self.eligible_for_source
            
            if not results:
                self.logger.info("No continue watching items from any Media client. Skipping %s...", self.source)
                return 0
            
            await asyncio.gather(*(client.scan(self.destination) for client in self.clients))
                
            self.logger.debug("Space usage: %.4g%% is below cache threshold: %.4g%%. Starting %s...", percent_used, self.cache_threshold, self.source)
            return threshold_bytes
        
        self.logger.info("Space usage: %.4g%% is above cache threshold: %.4g%%. Skipping %s...", percent_used, self.cache_threshold, self.source)
        return 0
    
    @cached_property
    def eligible_for_source(self) -> asyncio.Future[List[Tuple[str, Dict[str, str]]]]:
        pq: asyncio.PriorityQueue[Tuple[float, int, str]] = asyncio.PriorityQueue()
        seen: Set[str] = set()
        result: List[str] = []
        tasks = asyncio.gather(*(media.continue_watching(pq) for media in self.media))
        
        async def process():
            await tasks
            
            while not pq.empty():
                last_watched, _, path = await pq.get()
                
                if path in seen:
                    continue
                
                metadata: Dict[str, str] = {
                    "last_watched_at": str(datetime.fromtimestamp(-last_watched))
                }
                result.append((path, metadata))
                seen.add(path)
            
            return result
            
        return asyncio.create_task(process())
    
    def get_src_file(self, path: str) -> str:
        rel_path = os.path.relpath(path, self.destination)
        return os.path.join(self.source, rel_path)
        
    def get_dest_file(self, src_path: str) -> str:
        rel_path = os.path.relpath(src_path, self.source)
        return os.path.join(self.destination, rel_path)
        
    def pause(self, path: str) -> asyncio.Future:
        return asyncio.gather(*(qbit.pause(path) for qbit in self.clients))
            
    async def get_sort_key(self, path: str) -> Tuple[Tuple[int, int, int, float, int, int, int, float], Dict[str, str]]:
        # ignored path, no point checking
        if self.is_ignored(path):
            return ((1, 0, 0, 0, 0, 0, 0, 0), {})
        
        qbit_results: List[Tuple[int, int]]
        media_results: List[Tuple[bool, int]]
    
        qbit_results, media_results = await asyncio.gather(
            asyncio.gather(*(qbit.get_sort_key(path) for qbit in self.clients)),
            asyncio.gather(*(media.get_sort_key(path) for media in self.media))
        )

        torrent_eta: float = 0
        completion_age: int = 0
        num_seeders: int = 0
        
        for res in qbit_results:
            if not res:
                continue
            
            torrent_eta = max(max(eta for eta, _, _ in res), torrent_eta)
            completion_age = min(min(a for _, a, _ in res), completion_age)
            num_seeders = min(min(n for _, _, n in res), num_seeders)
            
        continue_watching, watched_left = any(cw for cw, _ in media_results), sum(wc for _, wc  in media_results)
        
        has_torrent = 1 if qbit_results else 0
        size = get_stat(path).st_size
        
        metadata: Dict[str, str] = {
            "continue_watching": str(continue_watching),
            "users_left_to_watch": str(watched_left),
            "num_torrents": str(len(qbit_results)),
            "torrent_eta": str(torrent_eta),
            "completion_age": f"{timedelta(seconds=completion_age).days}d",
            "num_seeders": str(num_seeders),
            "size": str(format_bytes_to_gib(size)),
            "age": get_age_str(path)
        }
        
        return ((
            # age_priority,             # 1. age_priority (0 if within age range, else 1)
            int(continue_watching), # 2. media un-watched -> 1, watched 0
            watched_left,           # 3. how many users left to watch
            has_torrent,            # 4. has_torrent (0 if has torrents, else 1)
            torrent_eta,            # 5. torrent eta, if any we postpone move, as we do not really need this file anyway soon?
            -completion_age,        # 6. -completion_age (negative to prioritize older completion age)
            -num_seeders,           # 7. -num_seeders (negative to prioritize more seeders)
            len(qbit_results),      # 8. num seeding torrents
            -size,                  # 9. bigger file goes first
            get_ctime(path)         # 10. ctime (file creation time as tiebreaker)
        ), metadata)
        
    def within_age_range(self, path: float):
        age = self.now.timestamp() - get_ctime(path)
        return self.min_age <= age <= self.max_age
    
    async def is_active(self, file: str) -> bool:
        tasks = [asyncio.create_task(media.is_active(file)) for media in self.media]
        
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
            f"       Media Clients: [{', '.join(map(str, self.media))}]\n"
            f"       Ignore patterns: [{', '.join(map(str, self.ignores))}]"
        )
        
    async def aclose(self):
        await asyncio.gather(*([media.aclose() for media in self.media] + [client.aclose() for client in self.clients]))