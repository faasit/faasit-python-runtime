import os
import time
from faasit_runtime.runtime.faasit_runtime import (
    FaasitRuntime,
    CallParams,
    StorageMethods,
    TellParams,
    CallResult,
    InputType,
    FaasitRuntimeMetadata,
)
from faasit_runtime.workflow import RouteRunner
from typing import Any, List
import asyncio

class LocalOnceRuntime(FaasitRuntime):
    name: str = 'local-once'
    def __init__(self, 
                 data, 
                 workflow_runner: RouteRunner = None, 
                 metadata: FaasitRuntimeMetadata = None) -> None:
        super().__init__()
        self._input = data
        self._workflow_runner = workflow_runner
        self._metadata = metadata
        self._storage = self.LocalStorage()

    def set_workflow(self, workflow_runner: RouteRunner):
        self._workflow_runner = workflow_runner
        return workflow_runner

    def input(self):
        return self._input

    def output(self, _out):
        return _out

    def call(self, fnName:str, fnParams: InputType) -> CallResult:
        fnParams: CallParams = CallParams(
            input=fnParams
        )
        event = fnParams.input
        seq = fnParams.seq
        if self._workflow_runner == None:
            raise Exception("workflow is not defined")
        metadata = self.helperCollectMetadata("call", fnName, fnParams)

        callerName = self._metadata.funcName
        print(f"[function call] {callerName} -> {fnName}")
        print(f"[call params] {event}")

        handler = self._workflow_runner.route(fnName)

        result = handler(event, self._workflow_runner, metadata)

        from faasit_runtime.durable import DurableWaitingResult
        if isinstance(result, DurableWaitingResult):
            result = result.waitResult()

        return result

    async def tell(self, fnName:str, fnParams: dict) -> Any:
        fnParams:TellParams = TellParams(**fnParams)
        event = fnParams.input
        if self._workflow_runner == None:
            raise Exception("workflow is not defined")
        metadata:FaasitRuntimeMetadata = self.helperCollectMetadata("tell", fnName, fnParams)
        # print(f"[Debug] {metadata.dict()}")
        callerName = self._metadata.funcName

        print(f"[function tell] {callerName} -> {fnName}")
        print(f"[tell params] {event}")
        async def task():
            handler = self._workflow_runner.route(fnName)
            nonlocal metadata
            await handler(event, self._workflow_runner, metadata)
            # from faasit_runtime.durable import DurableWaitingResult
            # if isinstance(result, DurableWaitingResult):
            #     result = await result.waitResult()
            # # callback
            # if metadata.invocation.callback.ctx['kind'] == 'durable-orchestrator-callback':
            #     handler = self._workflow_runner.route(callerName)
            #     callbackParams = TellParams(
            #         input=result,
            #         responseCtx=metadata.invocation.callback.ctx,
            #         callback=None
            #     )
            #     callbackMetadata = self.helperCollectMetadata("tell", callerName, callbackParams)
            #     result = await handler(result, self._workflow_runner, callbackMetadata)
            # return result
        subtask = asyncio.create_task(task())
        return subtask
        # return task
    
    @property
    def storage(self) -> StorageMethods:
        return self._storage
    
    class LocalStorage(StorageMethods):
        def __init__(self):
            self.storage_path = "./local_storage/"
            if not os.path.exists(self.storage_path):
                os.makedirs(self.storage_path)

        def put(self, filename, data: bytes) -> None:
            file_path = self.storage_path + filename
            dir_name = os.path.dirname(file_path)
            os.makedirs(dir_name, exist_ok=True)
            self._acquire_filelock(file_path)
            with open(file_path, "wb") as f:
                f.write(data)
                f.flush()
            print(f"[storage put] Put data into {file_path} successfully.")
            self._release_filelock(file_path)

        def get(self, filename, timeout = -1) -> bytes:
            file_path = self.storage_path + filename
            start_t = time.time()
            while not os.path.exists(file_path):
                time.sleep(0.001)
                if timeout > 0:
                    if time.time() - start_t > timeout / 1000: return None
            self._wait_filelock(file_path)
            while True:
                with open(file_path, "rb") as f:
                    data = f.read()
                data_len = len(data)
                if data_len == 0:
                    print(f"[storage get] read error of {file_path}, retry ...")
                    time.sleep(0.001)
                    continue
                break
            return data

        def list(self) -> List:
            return [f for f in os.listdir(self.storage_path) if not f.endswith(".lock")]

        def exists(self, filename: str) -> bool:
            file_path = self.storage_path + filename
            return os.path.exists(file_path)

        def delete(self, filename: str) -> None:
            file_path = self.storage_path + filename
            if os.path.exists(file_path):
                self._acquire_filelock(file_path)
                os.remove(file_path)
                print(f"[storage delete] Delete {file_path} successfully.")
                self._release_filelock(file_path)
            else:
                print(f"[storage delete] {file_path} is not exist.")

        # create our own simple file lock since we may debug in Windows environment
        def _acquire_filelock(self, file_path):
            lock_path = file_path + ".lock"
            os.makedirs(os.path.dirname(lock_path), exist_ok=True)
            while os.path.exists(lock_path):
                time.sleep(0.001)
            with open(lock_path, "wb") as f:
                f.write(bytes(1))

        def _release_filelock(self, file_path):
            lock_path = file_path + ".lock"
            if os.path.exists(lock_path):
                os.remove(lock_path)

        def _wait_filelock(self, file_path):
            lock_path = file_path + ".lock"
            while os.path.exists(lock_path):
                time.sleep(0.001)