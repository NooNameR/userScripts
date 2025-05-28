from abc import ABC, abstractmethod

class SeedingClient(ABC):
    @abstractmethod
    def pause(self, path: str):
        pass
    
    @abstractmethod    
    def resume(self):
        pass