from abc import ABC, abstractmethod
from typing import Set, Tuple

class SeedingClient(ABC):
    @abstractmethod
    def scan(self, root: str) -> None:
        pass
    
    @abstractmethod
    def pause(self, path: str) -> None:
        pass
    
    @abstractmethod
    def resume(self) -> None:
        pass
    
    @abstractmethod
    def get_sort_key(self, path: str) -> Set[Tuple[int, int]]:
        pass