from abc import ABC, abstractmethod

class MediaPlayer(ABC):
    @abstractmethod
    def is_active(self, file: str):
        pass
    
    @abstractmethod
    def is_not_watched(self, file: str) -> bool:
        pass