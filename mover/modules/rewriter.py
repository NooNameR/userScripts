import os
from abc import ABC, abstractmethod

class Rewriter(ABC):
    def __init__(self, source: str, destination: str):
        self.source = source
        self.destination = destination
    
    @abstractmethod
    def rewrite(self, root: str, path: str) -> str:
        pass
            
    @abstractmethod
    def restore(self, path: str) -> str:
        pass
    
    def on_source(self, path: str) -> str:
        return self.rewrite(self.source, path)
    
    def on_destination(self, path: str) -> str:
        return self.rewrite(self.destination, path)

class RealRewriter(Rewriter):
    def __init__(self, source: str, destination: str, _from: str, to: str):
        super().__init__(source, destination)
        
        rel_path = os.path.relpath(to, source)
        if rel_path == ".":
            rel_path = ""
        
        self.rel_path = rel_path.lstrip(os.sep)
        self._from = _from
        
    def restore(self, path: str) -> str:
        path = os.path.abspath(path)
        for root in (self.source, self.destination):
            try:
                if os.path.commonpath([path, root]) == root:
                    rel = os.path.relpath(path, os.path.join(root, self.rel_path))
                    return os.path.normpath(os.path.join(self._from, rel))
            except ValueError:
                continue
        return path

    def rewrite(self, root: str, path: str) -> str:
        path = os.path.abspath(path)
        try:
            rel = os.path.relpath(path, self._from)
            return os.path.normpath(os.path.join(root, self.rel_path, rel))
        except ValueError:
            return path
    
class NoopRewriter(Rewriter):
    def __init__(self, source: str, destination: str):
        super().__init__(source, destination)
    
    def rewrite(self, root: str, path: str) -> str:
        try:
            rel = os.path.relpath(path, self.source)
            return os.path.normpath(os.path.join(root, rel))
        except ValueError:
            return path
    
    def restore(self, path: str) -> str:
        path = os.path.abspath(path)
        for root in (self.source, self.destination):
            try:
                if os.path.commonpath([path, root]) == root:
                    rel = os.path.relpath(path, root)
                    return os.path.join(self.source, rel)
            except ValueError:
                continue
        return path