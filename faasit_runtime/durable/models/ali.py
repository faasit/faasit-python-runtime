from faasit_runtime.durable.runtime import DurableRuntime
from faasit_runtime.runtime.aliyun_runtime import AliyunRuntime
from ..state import (
    DurableStateClient,
    ScopedDurableStateClient,
    DurableFunctionState,
)

class ActorState(DurableStateClient):
    def __init__(self, instanceId, frt:AliyunRuntime):
        self._env = f"faasit-ali-{instanceId}"
        self._frt = frt

    def set(self,key:str,value):
        _state = self._frt._storage.get(self._env)
        if _state is None:
            _state = {}
        _state[key] = value
        self._frt._storage.put(self._env, _state)
    def get(self,key:str):
        _state = self._frt._storage.get(self._env)
        if _state is None:
            return None
        return _state.get(key, None)

def ali_durable(fn):
    def handler(event, context):
        def getClient(scopeId:str):
            client = frt._storage.get(scopeId)
            if client == None:
                return ScopedDurableStateClient.load(scopeId,{})
        frt = AliyunRuntime(event, context)
        params = frt.input()
        try:
            instanceId = params['instanceId']
        except KeyError as e:
            raise ValueError("Durable function `instanceId` is required")
        client = getClient(instanceId)
        state, init = DurableFunctionState.load(client)
        actorState = ActorState(instanceId, frt)
        df = DurableRuntime(
            frt=frt,
            durableMetadata=None,
            state=state,
            client=actorState
        )
        result = fn(df)
        # saveClient(client)
        return result
    return handler