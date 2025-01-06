from .metadata import Metadata
from .sending import PostUntil200
from .serverless_utils import Result, Address, TransportMode
from .redis_db import RedisProxy

import logging
from typing import Any, Optional, Union, Dict
import time

class ControllerMetadata(Metadata):
    def remote_call(self) -> Any:
        logging.debug(f"Remote_call {self.id}: {self.schedule[self.stage].ip}:{self.schedule[self.stage].port}")
        self.finish_time = 0
        self.retval = None
        self.call_time = time.time()
        self.unique_execution_id = f'{self.id}-uid-{self.call_cnt}'
        self.call_cnt += 1

        metadata = Metadata(self.execution_namespace, self.stage, self.schedule, self.trans_mode,
                            self.params, self.redis_proxy, self.remote_call_timeout, 
                            self.post_ratio)
        metadata.set_all(self.id, self.unique_execution_id, self.worker_cache, self.retval, self.call_cnt,
                         self.call_time, self.finish_time)

        try:
            return PostUntil200(f"http://{self.schedule[self.stage].ip}:{self.schedule[self.stage].port}", 
            {
                "type": "lambda-call",
                "metadata": metadata
            }, timeout = self.remote_call_timeout, post_ratio=self.post_ratio)
        except:
            self.call_time = 0
            raise

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

    def working(self) -> bool:
        if self.call_time == 0:
            raise Exception("remote_call() has not been called yet.")
        return not self.retval
    
    def fail(self) -> bool:
        if self.call_time == 0:
            raise Exception("remote_call() has not been called yet.")
        return isinstance(self.retval, Result.Err)

    def succ(self) -> bool:
        if self.call_time == 0:
            raise Exception("remote_call() has not been called yet.")
        return isinstance(self.retval, Result.Ok)

    def get_reply(self) -> Optional[Union[Result.Ok, Result.Err]]:
        if self.call_time == 0:
            raise Exception("remote_call() has not been called yet.")
        return self.retval
    
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