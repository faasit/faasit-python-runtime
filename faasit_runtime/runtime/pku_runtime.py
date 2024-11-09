from serverless_framework import (
    Metadata
)
from .faasit_runtime import (
    FaasitRuntime,
    FaasitRuntimeMetadata,
    StorageMethods
)
from typing import Dict, Optional

class PKURuntimeMetadata(FaasitRuntimeMetadata):
    metadata: Metadata
    


class PKURuntime(FaasitRuntime):
    def __init__(self, ft_metadata: FaasitRuntimeMetadata, metadata: Metadata):
        self._metadata: PKURuntimeMetadata = PKURuntimeMetadata(ft_metadata, metadata)
        self._input = metadata.params
        self._storage = self.PKUStorage(self._metadata)

    def input(self):
        return self._input
    
    def output(self,_out):
        return _out

    def call(self, fnName:str, fnParams: dict) -> dict:
        return

    def tell(self, fnName:str, fnParams: dict) -> dict:
        return

    def get_metadata(self) -> FaasitRuntimeMetadata:
        return self.metadata


    class PKUStorage(StorageMethods):
        class OutputOptions:
            def __init__(self, states: list[Optional[str]] = None, active_send: bool = False):
                self.dest_states = states
                self.active_send = active_send
        class InputOptions:
            def __init__(self, state: Optional[str], timeout: Optional[float] = None, active_pull: bool = True, tcp_direct: bool = True):
                self.src_state = state
                self.active_pull = active_pull
                self.tcp_direct = tcp_direct
                self.timeout = timeout

        def __init__(self, metadata: Metadata):
            super().__init__()
            self._metadata = metadata
        def put(self, filename: str, data: bytes, **opts) -> None:
            dest_states = opts.get('dest_states')
            active_send = opts.get('active_send')
            return self._metadata.output(dest_states, filename, data, active_send=active_send)

        def get(self, filename: str, **opts) -> bytes:
            src_state = opts.get('src_state')
            timeout = opts.get('timeout')
            active_pull = opts.get('active_pull')
            tcp_direct = opts.get('tcp_direct')
            return self._metadata.get_object(src_state, filename, timeout=timeout, active_pull=active_pull, tcp_direct=tcp_direct)

        def get_assert_exist(self, filename: str, **opts):
            src_state = opts.get('src_state')
            timeout = opts.get('timeout')
            active_pull = opts.get('active_pull')
            tcp_direct = opts.get('tcp_direct')
            obj = self._metadata.get_existed_object(src_state, filename, timeout=timeout, active_pull=active_pull, tcp_direct=tcp_direct)
            return obj

        def list(self) -> list:
            pass

        def exists(self, filename: str) -> bool:
            pass

        def delete(self, filename: str) -> None:
            pass