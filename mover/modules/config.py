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
    
        self.ignores = set(self.raw.get("ignore", []))
        self.mappings = [self.__parse_mapping(m) for m in self.raw.get("mappings", [])]
        
    def __parse_mapping(self, m) -> "MovingMapping":
        return MovingMapping(
            self.now,
            source = m["source"],
            destination = m["destination"],
            threshold = m.get("threshold", 0.0),
            min_age = parse(m.get("min_age", "2h")),
            max_age = parse(m.get("max_age")) if m.get("max_age") else float('inf'),
            clients = [QbitHelper(**client) for client in m.get("clients", [])],
            plex = [PlexHelper(**client) for client in m.get("plex", [])],
            ignores= self.ignores
        )
    
    def __str__(self) -> str:
        out = [
            f"Config:",
            f"  Ignore patterns: {self.ignores}",
            f"  Mappings:"
        ]
        for i, mapping in enumerate(self.mappings, 1):
            out.append(f"    {i}. {mapping}")
        return "\n".join(out)

class MovingMapping:   
    def __init__(self, now, source: str, destination: str, threshold: float, min_age: int, max_age: int, clients: list[QbitHelper], plex: list[PlexHelper], ignores: set[str]):
        self.source = source
        self.destination = destination
        self.threshold = threshold
        self.clients = clients
        self.plex = plex
        self.min_age = min_age
        self.max_age = max_age
        self.now = now
        self.ignores = ignores
        
    def needs_moving(self) -> bool:
        total, used, _ = shutil.disk_usage(self.source)
        percent_used = round((used / total) * 100, 4)
        
        if percent_used >= self.threshold:
            logging.debug("Space usage: %.4g%% is above moving threshold: %.4g%%. Starting %s...", percent_used, self.threshold, self.source)
            return True
        
        logging.info("Space usage: %.4g%% is below moving threshold: %.4g%%. Skipping %s...", percent_used, self.threshold, self.source)
        return False
    
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
            f"       Age range: {timedelta(seconds=self.min_age)} – {"..." if self.max_age == float('inf') else timedelta(seconds=self.max_age)}\n"
            f"       Clients: [{", ".join([str(helper) for helper in self.clients])}]\n"
            f"       Plex: [{", ".join([str(helper) for helper in self.plex])}]"
        )