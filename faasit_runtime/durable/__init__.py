from faasit_runtime.utils import (
    config
)

from faasit_runtime.durable.result import (
    DurableWaitingResult
)
def createOrchestratorScopedId(orcheId:str):
    return f"orchestrator::__state__::{orcheId}"



def durable(fn):
    conf = config.get_function_container_config()
    provider = conf['provider']
    if provider == 'local-once':
        from faasit_runtime.durable.models import localonce_durable
        return localonce_durable(fn)
    elif provider == 'local':
        return
    else:
        raise Exception(f"Unsupported provider {conf['provider']}")

__all__ = [
    "durable",
    "DurableWaitingResult",
]