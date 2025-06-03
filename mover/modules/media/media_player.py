from abc import ABC, abstractmethod
from enum import Enum
from asyncio import Queue
from typing import List, Tuple

class MediaPlayerType(Enum):
    PLEX = 1
    JELLYFIN = 2

class MediaPlayer(ABC):
    @abstractmethod
    def is_active(self, file: str):
        pass
    
    @abstractmethod
    def type(self) -> MediaPlayerType:
        pass
    
    @abstractmethod
    async def get_sort_key(self, path: str) -> int:
        pass
    
    @abstractmethod
    async def continue_watching(self, pq: Queue[Tuple[float, int, str]]) -> None:
        pass
    
    @abstractmethod
    async def aclose(self) -> None:
        pass