from serverless_utils import TransportMode, Address, Result, final_outputs_prefix
from typing import Dict, List, Any, Optional, Union, Set
from sending import PostUntil200, tcp_cache_get
from redis_db import RedisProxy
from kv_cache import KVCache
import random
import time
import logging
import os

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

    # called by controller
    def remote_call(self) -> Any:
        logging.debug(f"Remote_call {self.id}: {self.schedule[self.stage].ip}:{self.schedule[self.stage].port}")
        self.finish_time = 0
        self.retval = None
        self.call_time = time.time()
        self.unique_execution_id = f'{self.id}-uid-{self.call_cnt}'
        self.call_cnt += 1

        try:
            return PostUntil200(f"http://{self.schedule[self.stage].ip}:{self.schedule[self.stage].port}", 
            {
                "type": "lambda-call",
                "metadata": self
            }, timeout = self.remote_call_timeout, post_ratio=self.post_ratio)
        except:
            self.call_time = 0
            raise
    
    def _result_redis_key(self) -> str:
        return f"{self.unique_execution_id}-result"

    # called by controller
    def fetch_retval(self) -> bool:
        '''
        fetch success or failure status from redis
        return True if the status is fetched, otherwise False
        '''
        assert(self.redis_proxy)
        if not self.retval:
            self.retval = self.redis_proxy.extract(self._result_redis_key())

            if self.retval:
                self.finish_time = time.time()
                return True
            else:
                return False
        else:
            return True

    # called by controller
    def working(self) -> bool:
        if self.call_time == 0:
            raise Exception("remote_call() has not been called yet.")
        return not self.retval
    
    # called by controller
    def fail(self) -> bool:
        if self.call_time == 0:
            raise Exception("remote_call() has not been called yet.")
        return isinstance(self.retval, Result.Err)

    # called by controller
    def succ(self) -> bool:
        if self.call_time == 0:
            raise Exception("remote_call() has not been called yet.")
        return isinstance(self.retval, Result.Ok)

    # called by controller
    def get_reply(self) -> Optional[Union[Result.Ok, Result.Err]]:
        if self.call_time == 0:
            raise Exception("remote_call() has not been called yet.")
        return self.retval

    def namespace_obj_prefix(self) -> str:
        return f"{self.execution_namespace}-"

    # called by controller
    def cache_clear(self, timeout = 10) -> None:
        '''
        send cache-clear request to the worker
        '''
        try:
            PostUntil200(f"http://{self.schedule[self.stage].ip}:{self.schedule[self.stage].port}",
                {
                    "type": "cache-clear",
                    "prefix": self.namespace_obj_prefix()
                }, timeout = timeout, post_ratio=self.post_ratio)
        except Exception as e:
            logging.warning(f"Error occurred while sending cache-clear request to {self.id}: {str(e)}")


    # ================== below is the interface for the worker ==================


    def _through_redis(self, src_stage: Optional[str], dest_stage: Optional[str]) -> bool:
        if src_stage == None or dest_stage == None:
            return True
        
        return self.trans_mode is TransportMode.allRedis or \
            self.trans_mode is TransportMode.auto and self.schedule[dest_stage].ip != self.schedule[src_stage].ip

    # called by worker
    def output(self, dest_stages: List[Optional[str]], key: str, obj: Any, *, active_send: Optional[bool] = False) -> None:
        assert(self.worker_cache is not None)
        final_outputs_key = self.namespace_obj_prefix() + final_outputs_prefix + key
        key = self.namespace_obj_prefix() + key

        has_put_on_redis: bool = False
        has_put_on_worker_cache: bool = False

        for dest_stage in dest_stages:
            if self._through_redis(self.stage, dest_stage):
                if has_put_on_redis == False:
                    has_put_on_redis = True
                    assert(self.redis_proxy is not None)
                    if dest_stage is None:
                        # Final outputs.
                        outputs_key = final_outputs_key
                    else:
                        outputs_key = key
                    self.redis_proxy.put(outputs_key, obj)
            else:
                if active_send:
                    assert(dest_stage is not None)
                    PostUntil200(f"http://{self.schedule[dest_stage].ip}:{self.schedule[dest_stage].port}", 
                        {
                            "type": "cache-put",
                            "key": key,
                            "value": obj
                        })
                else:
                    if has_put_on_worker_cache == False:
                        has_put_on_worker_cache = True
                        logging.debug(f"Output {key} to {dest_stage} is buffered in the local cache.")
                        self.worker_cache.put(key, obj)

    # called by worker
    def get_object(self, src_stage: Optional[str], key: str, timeout: Optional[float] = None, active_pull: bool = True, tcp_direct: bool = True) -> Optional[Any]:
        '''
        tcp_direct: if True, the function will not use http to fetch the object
        '''
        assert(self.worker_cache is not None)

        if src_stage:
            key = self.namespace_obj_prefix() + key
        # Otherwise it is a global redis key

        if self._through_redis(src_stage, self.stage):
            assert(self.redis_proxy is not None)
            return self.redis_proxy.get(key, timeout = timeout)
        else:
            if active_pull:
                assert(src_stage is not None)
                if tcp_direct:
                    return tcp_cache_get(self.schedule[src_stage].ip, self.schedule[src_stage].cache_port, key, timeout = timeout or 5)
                else:
                    retval = PostUntil200(f"http://{self.schedule[src_stage].ip}:{self.schedule[src_stage].port}",
                        {
                            "type": "cache-get",
                            "key": key,
                        }, timeout = timeout or 5, post_ratio=self.post_ratio)
                    return retval
            else:
                return self.worker_cache.get(key, timeout = timeout)

    def get_existed_object(self, src_stage: Optional[str], key: str, timeout: Optional[float] = None, active_pull: bool = True, tcp_direct: bool = True) -> Any:
        retval = self.get_object(src_stage, key, timeout, active_pull, tcp_direct)
        if retval is None:
            raise Exception(f"The request for key {key} unwraps failed: no such entry")
        return retval

    # called by worker
    def update_status(self, status: Union[Result.Ok, Result.Err]) -> None:
        assert(self.redis_proxy is not None)
        self.redis_proxy.put(self._result_redis_key(), status)

    def tmp_folder(self) -> str:
        retval = f"/tmp/{self.unique_execution_id}/"
        os.makedirs(retval, exist_ok = True)
        return retval