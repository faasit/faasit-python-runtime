from .faasit_runtime import (
    FaasitRuntime, 
    InputType,
    CallResult,
    StorageMethods
)
import requests
import os
import json
import uuid
from faasit_runtime.serverless_function import Metadata
from faasit_runtime.utils.logging import log
from faasit_runtime.storage import RedisDB


class KnativeRuntime(FaasitRuntime):
    name: str = 'knative'
    def __init__(self,metadata: Metadata) -> None:
        super().__init__()
        self._input = metadata._params
        self._id = metadata._id
        self._redis_db = metadata._redis_db
        self._namespace = metadata._namespace
        self._router:dict = metadata._router
        self._type = metadata._type
        self._storage = self.KnStorage(redis_db=self._redis_db)
    
    @property
    def storage(self) -> "KnStorage":
        return self._storage

    def input(self):
        result = None
        try:
            result = json.loads(self._input)
        except Exception as e:
            result = self._input
        return result
    def output(self, _out):
        return _out

    def _collect_metadata(self, params, call_kind='invoke'):
        id = str(uuid.uuid4())
        
        return {
            'id': id,
            'params': params,
            'namespace': self._namespace,
            'router': self._router,
            'type': call_kind
        }
    def call(self, fnName:str, fnParams: InputType) -> CallResult:
        call_url = self._router.get(fnName)
        if call_url is None:
            raise ValueError(f"Function {fnName} not found in router")
        
        metadata_dict = self._collect_metadata(params=fnParams)
        log.info(f"Calling function {fnName} with params metadata: {metadata_dict}")

        resp = requests.post(f"{call_url}", json=metadata_dict, headers={'Content-Type': 'application/json'}, proxies={'http': None, 'https': None})

        log.info(f"Response from function {fnName}: {resp}")
        resp = resp.json()
        if resp['status'] == 'error':
            raise ValueError(f"Failed to call function {fnName}: {resp['error']}")
        return resp['data']
    
    def tell(self, fnName:str, fnParams: InputType) -> CallResult:
        call_url = self._router.get(fnName)
        if call_url is None:
            raise ValueError(f"Function {fnName} not found in router")
        
        metadata_dict = self._collect_metadata(params=fnParams, call_kind='tell')
        log.info(f"Sending message to function {fnName} with metadata: {metadata_dict}")
        resp = requests.post(f"{call_url}", json=metadata_dict, headers={'Content-Type': 'application/json'}, proxies={'http': None, 'https': None})
        log.info(f"Response from function {fnName}: {resp}")
        resp = resp.json()
        if resp['status'] == 'error':
            raise ValueError(f"Failed to call function {fnName}: {resp['error']}")
        return resp['message']
        # return self._msg_sender.send(topic=fnName, body=json.dumps(metadata_dict))

    class KnStorage(StorageMethods):
        def __init__(self, redis_db: RedisDB):
            self._redis_db = redis_db
        def get(self, filename: str, *_, **__):
            return self._redis_db.get(filename)
        def put(self, filename: str, value, *_, **__):
            return self._redis_db.set(filename, value)
        def delete(self, filename: str):
            return self._redis_db.delete(filename)