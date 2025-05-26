import pyaml_env
import fnmatch
import os
import time
import shutil
import logging
from .plex_helper import PlexHelper
from .qbit_helper import QbitHelper
from .helpers import get_ctime
from datetime import timedelta
from pytimeparse2 import parse

class Config:
    def __init__(self, path='config.yaml'):
        self.now = time.time()
        self.raw = pyaml_env.parse_config(path)
    
        self.mappings = [self.__parse_mapping(m) for m in self.raw.get("mappings", [])]
        
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
    def __init__(self, now, raw):
        self.now = now
        self.source = raw["source"]
        self.destination = raw["destination"]
        self.threshold = raw.get("threshold", 0.0)
        self.cache_threshold = raw.get("cache_threshold", 0.0)
        self.min_age = parse(raw.get("min_age", "2h"))
        self.max_age = parse(raw.get("max_age")) if raw.get("max_age") else float('inf')
        self.clients = [QbitHelper(self.source, **client) for client in raw.get("clients", [])]
        self.plex = [PlexHelper(self.source, **client) for client in raw.get("plex", [])]
        self.ignores = set(raw.get("ignore", []))
        
    def needs_moving(self) -> bool:
        total, used, _ = shutil.disk_usage(self.source)
        percent_used = round((used / total) * 100, 4)
        
        if percent_used >= self.threshold:
            logging.debug("Space usage: %.4g%% is above moving threshold: %.4g%%. Starting %s...", percent_used, self.threshold, self.source)
            return True
        
        logging.info("Space usage: %.4g%% is below moving threshold: %.4g%%. Skipping %s...", percent_used, self.threshold, self.source)
        return False
    
    def can_move_to_source(self) -> bool:
        if not self.cache_threshold:
            return False
        
        total, used, _ = shutil.disk_usage(self.source)
        percent_used = round((used / total) * 100, 4)
        
        if percent_used <= self.cache_threshold:
            logging.debug("Space usage: %.4g%% is belowe cache threshold: %.4g%%. Starting %s...", percent_used, self.cache_threshold, self.source)
            return True
        
        logging.info("Space usage: %.4g%% is above cache threshold: %.4g%%. Skipping %s...", percent_used, self.cache_threshold, self.source)
        return False
    
    def eligible_for_source(self) -> set[str]:
        result = []
        
        for file in [i for plex in self.plex for i in plex.continue_watching]:
            rel_path = os.path.relpath(file, self.source)
            path = os.path.join(self.destination, rel_path)
            
            if os.path.exists(path):
                result.append(path)
        
        return result
    
    def get_src_file(self, path: str) -> str:
        rel_path = os.path.relpath(path, self.destination)
        return os.path.join(self.source, rel_path)
        
    def get_dest_file(self, src_path: str) -> str:
        rel_path = os.path.relpath(src_path, self.source)
        return os.path.join(self.destination, rel_path)
        
    def pause(self, path: str) -> None:
        for qbit in self.clients:
            qbit.pause(path)
            
    def resume(self) -> None:
        for qbit in self.clients:
            qbit.resume()
            
    def is_watched(self, file: str) -> bool:
        for plex in self.plex:
            if plex.is_watched(file):
                return True
        return False
    
    def is_active(self, file: str) -> bool:
        for plex in self.plex:
            if plex.is_active(file):
                return True
        return False
        
    def is_ignored(self, path: str) -> bool:
        if not self.ignores:
            return False
        
        return any(fnmatch.fnmatch(path, pattern) for pattern in self.ignores)
    
    def is_file_within_age_range(self, filepath: str) -> bool:
        file_age = self.now - get_ctime(filepath)
        return self.min_age <= file_age <= self.max_age
    
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