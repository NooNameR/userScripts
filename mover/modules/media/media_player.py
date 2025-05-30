from abc import ABC, abstractmethod
from enum import Enum
from typing import List

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
    def get_sort_key(self, path: str) -> int:
        pass
    
    @abstractmethod
    def continue_watching(self) -> List[str]:
        pass