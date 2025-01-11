from faasit_runtime.utils.config import get_function_container_config
from faasit_runtime.runtime import (
    FaasitRuntimeMetadata,
    TellParams,
    FaasitRuntime
)

def callback(result,frt:FaasitRuntime):
    metadata = frt.metadata()
    if len(metadata.stack) == 0:
        return result
    lastInvocation = metadata.stack.pop()
    if lastInvocation.callback == None:
        return result
    if lastInvocation.callback.ctx.get('kind') == 'durable-orchestrator-callback':
        callbackParams = TellParams(
            input=result,
            callback=None,
            responseCtx=lastInvocation.callback.ctx
        )
        result = frt.tell(lastInvocation.caller.funcName, callbackParams.dict())
        return result
    
def popStack(result, metadata: FaasitRuntimeMetadata):
    if len(metadata.stack) == 0:
        return result
    lastInvocation = metadata.stack[-1]
    metadata.stack.pop()
    if lastInvocation.kind == 'tell':
        return result

__all__ = [
    "get_function_container_config",
    "callback",
    "popStack"
]