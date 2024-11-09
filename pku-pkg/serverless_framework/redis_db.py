import redis
import pickle
import time
from typing import Any, List, Optional
import logging
import os

class RedisProxy():
    def __init__(self, host: str, port: int, *, password: str):
        self.redis_host = host
        self.redis_port = port
        self.pool = redis.ConnectionPool(host=self.redis_host, port=self.redis_port, password=password)
    
    def put(self, key: str, value: Any):
        r = redis.Redis(connection_pool=self.pool)
        value = pickle.dumps(value)
        
        if r.set(key, value) is not True:
            raise Exception(f"Redis put failed: {key} {value}")

        logging.debug(f"Redis put succeeds: {key} len = {len(value)}")

    def extract(self, key: str) -> Optional[Any]:
        r = redis.Redis(connection_pool=self.pool)
        value = r.get(key)
        if value is not None and isinstance(value, bytes):
            value = pickle.loads(value)
            if r.delete(key) == 0:
                return None
        return value

    def get(self, key: str, timeout: Optional[float] = None) -> Optional[Any]:
        '''
        Get the value of key, blocking.
        If the key does not exist until timeout, None is returned.
        '''
        r = redis.Redis(connection_pool=self.pool)
        value = r.get(key)
        
        while value is None and timeout is not None:
            time.sleep(0.1)
            timeout -= 0.1
            if timeout <= 0:
                logging.warning(f"Redis get timeout: {key}")
                return None
            value = r.get(key)
        
        if not value:
            return None
        logging.debug(f"Redis get succeeds: {key} len = {len(value)}")
        return pickle.loads(value)

    def remove(self, key) -> int:
        r = redis.Redis(connection_pool=self.pool)
        return r.delete(key)

    def get_keys_by_prefix(self, prefix) -> List[str]:
        r = redis.Redis(connection_pool=self.pool)
        return [s.decode('utf-8') for s in r.keys(prefix+'*')]

    def delete_keys_by_prefix(self, prefix) -> int:
        r = redis.Redis(connection_pool=self.pool)
        key = r.keys(prefix+'*')
        if key:
            return r.delete(*key)
        return 0

    def dump_keys_by_prefix(self, prefix, output_folder : str):
        r = redis.Redis(connection_pool=self.pool)
        os.makedirs(output_folder, exist_ok=True)
        if not output_folder.endswith('/'):
            output_folder += '/'
        keys = r.keys(prefix+'*')
        for key in keys:
            keystr = key.decode('utf-8')
            filename = output_folder + keystr
            obj = self.get(keystr)
            mode = None
            if type(obj) == str:
                mode = 'w'
            elif type(obj) == bytes or type(obj) == bytearray:
                mode = 'wb'
            else:
                raise Exception(f"Type {type(obj)} not supported in dump_keys_by_prefix with prefix: {prefix}")
            with open(filename, mode) as f:
                f.write(obj)
        
            

    
    def clear(self):
        r = redis.Redis(connection_pool=self.pool)
        r.flushall()

    # def wait_for(self, key, timeout = 10) -> bool:
    #     r = redis.Redis(connection_pool=self.pool)
    #     start_time = time.time()
    #     while key not in r.keys():
    #         time.sleep(0.1)
    #         if time.time() - start_time > timeout:
    #             return False
    #     return True

    # @DeprecationWarning
    # def wait_for_prefix(self, key, timeout=60):
    #     r = redis.Redis(connection_pool=self.pool)

    #     # todo: fix this
    #     start_time = time.time()
    #     while len(r.keys(key+'*')) == 0:
    #         time.sleep(0.1)
    #         if time.time() - start_time > timeout:
    #             raise Exception(f"Redis timeout waiting for key prefix {key}")
    #     return True
