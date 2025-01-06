from .metadata import Metadata
from .serverless_utils import TransportMode, Result, final_outputs_prefix
from .sending import PostUntil200, tcp_cache_get

from typing import Optional, List, Any, Union
import logging
import os

class WorkerMetadata(Metadata):
    def _through_redis(self, src_stage: Optional[str], dest_stage: Optional[str]) -> bool:
        if src_stage == None or dest_stage == None:
            return True
        
        return self.trans_mode is TransportMode.allRedis or \
            self.trans_mode is TransportMode.auto and self.schedule[dest_stage].ip != self.schedule[src_stage].ip

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

    def update_status(self, status: Union[Result.Ok, Result.Err]) -> None:
        assert(self.redis_proxy is not None)
        logging.debug(f"Update status: {status}, id: {self.id}, stage: {self.stage}, key: {self._result_redis_key()}")
        self.redis_proxy.put(self._result_redis_key(), status)

    def tmp_folder(self) -> str:
        retval = f"/tmp/{self.unique_execution_id}/"
        os.makedirs(retval, exist_ok = True)
        return retval