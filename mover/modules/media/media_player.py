from abc import ABC, abstractmethod
from enum import Enum

class MediaPlayerType(Enum):
    PLEX = 1
    JELLYFIN = 2

class MediaPlayer(ABC):
    @abstractmethod
    def is_active(self, file: str):
        pass
    
    @abstractmethod
    def is_not_watched(self, file: str) -> bool:
        pass
    
    @abstractmethod
    def type(self) -> MediaPlayerType:
        pass