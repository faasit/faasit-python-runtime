from serverless_framework import (
    WorkerMetadata
)
from ..runtime import DurableRuntime
from faasit_runtime.runtime import (
    load_runtime
)
from ..metadata import DurableMetadata
from ..state import (
    ScopedDurableStateClient,
    DurableFunctionState,
    DurableStateClient
)
from faasit_runtime import createFaasitRuntimeMetadata

class ActorState(DurableStateClient):
    def __init__(self, lambda_id, instanceId, md:WorkerMetadata):
        self._env = {
            "LambdaId": lambda_id,
            "InstanceId": "t"+str(instanceId),
            "LogTable": lambda_id + "-log",
            "IntentTable": lambda_id + "-collector",
            "LocalTable": lambda_id + "-local",
            "StepNumber": 0,
        }
        self._md = md
    def set(self,key:str,value):
        self._md.set_state(self._env, key, value)
    def get(self,key:str):
        self._md.get_state(self._env, key)

def pku_durable(fn):
        
    def handler(md:WorkerMetadata):
        def getClient(scopeId:str):
            client = md.redis_proxy.get(scopeId)
            if client == None:
                return ScopedDurableStateClient.load(scopeId,{})
        def saveClient(client: ScopedDurableStateClient):
            md.redis_proxy.put(client._scopedId,client)
        params = md.params
        try:
            instanceId = params['instanceId']
            lambdaId = params['lambdaId']
        except KeyError as e:
            raise ValueError("Durable function `instanceId` & `lambdaId` is required")
        PkuRuntime = load_runtime('pku')
        frt = PkuRuntime(createFaasitRuntimeMetadata('stage'), md)
        client = getClient(instanceId)
        state, init = DurableFunctionState.load(client)
        actorState = ActorState(lambdaId, instanceId, md)
        df = DurableRuntime(
            frt=frt,
            durableMetadata=None,
            state=state,
            client=actorState
        )
        result = fn(df)
        saveClient(client)
        return result
    return handler
