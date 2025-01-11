from .metadata import FaasitRuntime,DurableMetadata
from .state import (
    DurableFunctionState,
    Action,
    DurableStateClient
)
from .context import (
    DurableCallbackContext
)
from faasit_runtime.runtime import (
    InputType,
    TellParams
)

class DurableException(Exception):
    def __init__(self, task) -> None:
        super().__init__()
        self.task = task
    pass

class DurableRuntime(FaasitRuntime):
    def __init__(self, 
                 frt: FaasitRuntime,
                 durableMetadata: DurableMetadata,
                 state: DurableFunctionState,
                 client:DurableStateClient):
        self._pc = 0
        self._frt = frt
        self._state = state
        self._durable_metadata = durableMetadata
        self._client = client

    def state(self):
        return self._state
    def input(self):
        return self._frt.input()
    def output(self, _out):
        return self._frt.output(_out)
    def metadata(self):
        return self._durable_metadata
    
    @property
    def storage(self):
        return self._frt.storage

    def call(self, fnName:str, fnParams: InputType):
        self._pc += 1
        pc = self._pc

        if pc <= len(self._state._actions):
            # already called
            task = self._state._actions[pc-1]
            if task.status == 'completed' and task.kind == 'call':
                return task.result
            else:
                raise Exception("unreachable code")
        
        callback = TellParams.Callback(
            ctx=DurableCallbackContext(
                kind='durable-orchestrator-callback',
                orchestrator=self._durable_metadata.orchestrator,
                taskpc=pc,
                
            )
        )
        fnParams:TellParams = TellParams(
            input=fnParams,
            callback=callback,
            responseCtx=self._durable_metadata.runtimeData
        )
        task = self._frt.tell(fnName, fnParams.dict())
        self._state.add_action(Action(kind='call', status='pending', result={}))

        raise DurableException(task)

    async def tell(self, fnName:str, fnParams: TellParams):
        return await self._frt.tell(fnName, fnParams)
    
    def setState(self, key, value):
        self._client.set(key, value)
    def getState(self, key):
        return self._client.get(key)