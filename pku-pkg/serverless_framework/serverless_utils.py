from enum import Enum, auto
from collections import namedtuple
from typing import Any, List, Optional, Dict


class TransportMode(Enum):
    allTCP = auto()
    allRedis = auto()
    auto = auto() # local tcp, remote redis

class RuntimeType(Enum):
    default = auto()
    runvk = auto()

Address = namedtuple('Address', ['ip', 'port', 'cache_port'])
LockPair = namedtuple('LockPair', ['lock', 'value'])
final_outputs_prefix : str = '__final_outputs__'

class ObjBytesPair:
    def __init__(self, obj: Any, bytes: Optional[bytes]):
        self.obj = obj
        self.bytes = bytes

    def __str__(self):
        return f"ObjBytesPair({str(self.obj)}, {str(self.bytes)})"

    def __repr__(self):
        return str(self)

class Result:
    class Ok:
        def __init__(self, value: Any):
            self.value = value
        
        def __str__(self):
            return f"Result.Ok({str(self.value)})"

        def __repr__(self):
            return str(self)

    class Err:
        def __init__(self, exception: Any):
            self.exception = exception

        def __str__(self):
            return f"Result.Err({str(self.exception)})"

        def __repr__(self):
            return str(self)


class DuplicatedPortChecker:
    def __init__(self):
        self.ports: Dict[int, str] = {}

    def check(self, port: int) -> bool:
        if port in self.ports:
            return False
        self.ports[port] = ''
        return True
    
    def check_interval(self, start: int, cnt: int) -> bool:
        for i in range(cnt):
            if not self.check(start + i):
                return False
        return True
    
    def insert(self, port: int, owner: str = 'anonymous'):
        assert(port not in self.ports)
        self.ports[port] = owner

    def insert_interval(self, start: int, cnt: int, owner: str = 'anonymous'):
        for i in range(cnt):
            assert(start + i not in self.ports)
            self.insert(start + i, owner)