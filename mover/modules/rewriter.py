import os
from abc import ABC, abstractmethod

class Rewriter(ABC):
    def __init__(self, source: str, destination: str):
        self.source = source
        self.destination = destination
    
    @abstractmethod
    def _rewrite(self, root: str, path: str) -> str:
        pass
    
    def on_source(self, path: str) -> str:
        return self._rewrite(self.source, path)
    
    def on_destination(self, path: str) -> str:
        return self._rewrite(self.destination, path)

class RealRewriter(Rewriter):
    def __init__(self, source: str, destination: str, _from: str, to: str):
        super().__init__(source, destination)
        
        rel_path = os.path.relpath(to, source)
        if rel_path == ".":
            rel_path = ""
        
        self.rel_path = rel_path.lstrip(os.sep)
        self._from = _from

    def _rewrite(self, root: str, path: str) -> str:
        rel_path = os.path.relpath(path, self._from)
        return os.path.join(root, self.rel_path, rel_path.lstrip(os.sep))
    
class NoopRewriter(Rewriter):
    def __init__(self, source: str, destination: str):
        super().__init__(source, destination)
    
    def _rewrite(self, root: str, path: str) -> str:
        rel_path = os.path.relpath(path, self.source)
        return os.path.join(root, rel_path)