from ..utils import (
    config
)

def createOrchestratorScopedId(orcheId:str):
    return f"orchestrator::__state__::{orcheId}"

def localonce(fn):
    from .models.localonce import localonce_durable
    return localonce_durable(fn)

def local(fn):
    return

def pku(fn):
    from .models.pku import pku_durable
    return pku_durable(fn)


def kn(fn):
    from .models.knative import kn_durable
    return kn_durable(fn)

def durable_helper(fn):
    conf = config.get_function_container_config()
    provider = conf['provider']
    providers = {
        'local-once': localonce,
        'local': local,
        'pku': pku,
        'knative': kn,
    }
    try:
        return providers[provider](fn)
    except KeyError:
        raise ValueError(f"Invalid provider {provider}")

__all__ = [
    "durable_helper"
]