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
    match provider:
        case 'aliyun':
            from .aliyun_runtime import AliyunRuntime
            return AliyunRuntime
        case 'local':
            from .local_runtime import LocalRuntime
            return LocalRuntime
        case 'local-once':
            from .local_once_runtime import LocalOnceRuntime
            return LocalOnceRuntime
        case 'knative':
            from .kn_runtime import KnativeRuntime
            return KnativeRuntime
        case 'pku':
            from .pku_runtime import PKURuntime
            return PKURuntime
        case _:
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