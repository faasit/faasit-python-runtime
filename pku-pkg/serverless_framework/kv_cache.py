import threading
import logging
from typing import Any, Optional, Dict
import pickle
from .serverless_utils import ObjBytesPair

class KVCache:
    def __init__(self):
        self.cv = threading.Condition()
        self.inmem_cache: Dict[str, ObjBytesPair] = {}
        pass

    def put(self, key: str, value: Any):
        with self.cv:
            self.inmem_cache[key] = ObjBytesPair(value, None)
            self.cv.notify_all()

    def extract(self, key: str) -> Any:
        with self.cv:
            value = self.inmem_cache[key].obj
            if value is None:
                raise KeyError(key)
            del self.inmem_cache[key]
            return value
    
    def _get_pairs(self, key: str, timeout: Optional[float] = None) -> Optional[ObjBytesPair]:
        if timeout == None:
            with self.cv:
                return self.inmem_cache.get(key, None)
            
        with self.cv:
            while key not in self.inmem_cache.keys():
                # TODO: it can be better, but now it works.
                self.cv.wait(timeout=0.1)
                timeout -= 0.1
                if timeout <= 0:
                    logging.warning(f"KVCache get timeout: {key}")
                    return None
            return self.inmem_cache[key]


    def get(self, key: str, timeout: Optional[float] = None) -> Optional[Any]:
        retval = self._get_pairs(key, timeout)
        if retval is None:
            return None
        return retval.obj
        
    def get_bytes(self, key: str, timeout: Optional[float] = None) -> Optional[bytes]:
        retval = self._get_pairs(key, timeout)
        if retval is None:
            return None
        if retval.bytes is None:
            bytes = pickle.dumps(retval.obj)
            with self.cv:
                retval.bytes = bytes
        return retval.bytes

    def remove(self, key: str):
        with self.cv:
            if key in self.inmem_cache:
                del self.inmem_cache[key]
            else:
                raise KeyError(key)

    def get_keys_by_prefix(self, prefix: str) -> list:
        with self.cv:
            return [key for key in self.inmem_cache.keys() if key.startswith(prefix)]


    def clear_with_prefix(self, prefix: str) -> int:
        retval = 0
        with self.cv:
            for key in list(self.inmem_cache.keys()): # the list() is important.
                if key.startswith(prefix):
                    retval += 1
                    del self.inmem_cache[key]
        return retval