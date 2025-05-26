import os
from abc import ABC, abstractmethod

class Rewriter(ABC):
    @abstractmethod
    def rewrite(self, path: str) -> str:
        pass

class RealRewriter(Rewriter):
    def __init__(self, root: str, _from: str, to: str):
        self.root = root
        self._from = _from
        self._to = to
            
    def rewrite(self, path: str) -> str:
        rel_path = os.path.relpath(path, self._from)
        return os.path.join(self._to, rel_path.lstrip(os.sep))
    
class NoopRewriter(Rewriter):
    def rewrite(self, path: str) -> str:
        return path