from abc import ABC, abstractmethod
from typing import Set, Tuple

class SeedingClient(ABC):
    @abstractmethod
    async def scan(self, root: str) -> None:
        pass
    
    @abstractmethod
    async def pause(self, path: str) -> None:
        pass
    
    @abstractmethod
    async def get_sort_key(self, path: str) -> Set[Tuple[float, int, int]]:
        pass
    
    @abstractmethod
    async def aclose() -> None:
        pass