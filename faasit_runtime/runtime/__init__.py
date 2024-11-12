from faasit_runtime.runtime.faasit_runtime import (
    FaasitRuntime,
    createFaasitRuntimeMetadata,
    FaasitResult,
    FaasitRuntimeMetadata,
    CallResult,
    InputType,
    TellParams
)


def load_runtime(provider) -> FaasitRuntime:
    if provider == 'aliyun':
        from .aliyun_runtime import AliyunRuntime
        return AliyunRuntime
    elif provider == 'local':
        from .local_runtime import LocalRuntime
        return LocalRuntime
    elif provider == 'local-once':
        from .local_once_runtime import LocalOnceRuntime
        return LocalOnceRuntime
    elif provider == 'knative':
        from .kn_runtime import KnativeRuntime
        return KnativeRuntime
    elif provider == 'pku':
        from .pku_runtime import PKURuntime
        return PKURuntime
    else:
        raise ValueError(f"Invalid provider {provider}")

__all__ = [
    "FaasitRuntime",
    "createFaasitRuntimeMetadata",
    "FaasitResult",
    "FaasitRuntimeMetadata",
    "CallResult",
    "InputType",
    "TellParams",
    "load_runtime"
]