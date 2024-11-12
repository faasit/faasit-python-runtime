from .faasit_runtime import (
    FaasitRuntime,
    StorageMethods
)

class K8sRuntime(FaasitRuntime):
    name: str = "k8s"
    def __init__(self, data, metadata):
        super().__init__()
        self._input = data
        self._matadata = metadata
        self._storage = self.K8sStorage()
    def input(self):
        return self._input
    def output(self, _out):
        return {
            'status': "finished",
            'result': _out
        }
    def metadata(self):
        return self._matadata
    
    def call(self, fnName: str, fnParams):
        pass

    class K8sStorage(StorageMethods):
        def put(self, filename: str, data: bytes) -> None:
            pass

        def get(self, filename: str, timeout = -1) -> bytes:
            pass

        def list(self) -> List:
            pass

        def exists(self, filename: str) -> bool:
            pass

        def delete(self, filename: str) -> None:
            pass