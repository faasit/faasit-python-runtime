from typing import Dict, List, Any, Optional, Union, Set
import random

from .serverless_utils import TransportMode, Address, Result
from .redis_db import RedisProxy
from .kv_cache import KVCache

'''
Metadata is the ONLY interface between the worker and the framework.
Metadata will be initialized by the controller, and passed to the worker.
'''


class Metadata:
    def __init__(self, 
            execution_namespace: str, # f"{app-name}-{engine-id}" for now
            stage: str,
            schedule: Dict[str, Address], 
            trans_mode: TransportMode,
            params: Dict[str, Any],
            redis_proxy: RedisProxy,
            remote_call_timeout: float,
            post_ratio: float = 0.0,
        ):

        self.stage = stage
        self.schedule = schedule
        self.trans_mode = trans_mode
        self.params = params
        self.retval: Optional[Union[Result.Ok, Result.Err]] = None
        self.execution_namespace = execution_namespace
        self.remote_call_timeout = remote_call_timeout
        self.post_ratio = post_ratio


        # high-level object, which cannot be serialized
        self.redis_proxy: Optional[RedisProxy] = redis_proxy
        self.worker_cache: Optional[KVCache] = None

        # id is used to identify a series of tries of the same lambda function launched by this metadata
        self.id =  f'{self.execution_namespace}-{self.stage}-{random.randint(0, 100000)}'

        # unique_execution_id is used to identify a run (i.e. a remote_call) of a lambda function
        self.unique_execution_id: Optional[str] = None

        self.call_cnt = 0
        self.call_time = 0
        self.finish_time = 0

        # sanity check
        if stage not in schedule:
            raise Exception(f"stage {stage} is not in schedule {schedule}")
    
    def __getstate__(self) -> Dict[str, Any]:
        state = self.__dict__.copy()
        del state['redis_proxy']
        del state['worker_cache']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.redis_proxy = None
        self.worker_cache = None

    def _result_redis_key(self) -> str:
        return f"{self.unique_execution_id}-result"

    def namespace_obj_prefix(self) -> str:
        return f"{self.execution_namespace}-"

    def set_all(self, _id: str, _unique_execution_id: str, 
                _worker_cache: KVCache, _retval: Union[Result.Ok, Result.Err], 
                _call_cnt: int, _call_time: float, _finish_time: float) -> None:
        self.id = _id
        self.unique_execution_id = _unique_execution_id
        self.worker_cache = _worker_cache
        self.retval = _retval
        self.call_cnt = _call_cnt
        self.call_time = _call_time
        self.finish_time = _finish_time

    def set_id(self, _id: str) -> None:
        self.id = _id
    
    def set_unique_excutioin_id(self, _id: str) -> None:
        self.unique_execution_id = _id
    
    def set_worker_cache(self, worker_cache: KVCache) -> None:
        self.worker_cache = worker_cache
    
    def set_retval(self, retval: Union[Result.Ok, Result.Err]) -> None:
        self.retval = retval

    def set_call_cnt(self, call_cnt: int) -> None:
        self.call_cnt = call_cnt
    
    def set_call_time(self, call_time: float) -> None:
        self.call_time = call_time
    
    def set_finish_time(self, finish_time: float) -> None:
        self.finish_time = finish_time

    