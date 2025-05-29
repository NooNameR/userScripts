from abc import ABC, abstractmethod
from typing import Set, Tuple

class SeedingClient(ABC):
    @abstractmethod
    def pause(self, path: str):
        pass
    
    @abstractmethod
    def resume(self):
        pass
    
    @abstractmethod
    def get_sort_key(self, path: str) -> Set[Tuple[int, int]]:
        pass