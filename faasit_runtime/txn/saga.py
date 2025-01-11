import uuid,time
from pydantic import BaseModel
from typing import Any,Callable,Tuple
class SagaTask:
    def __init__(self, 
                 operateFn, compensateFn):
        # saga belongs to
        self.txnID = None
        self.result = None
        self.error:str = ""
        self.operateFn = operateFn
        self.compensateFn = compensateFn
    def operate(self, txnID: str, payload):
        self.txnID = txnID
        try:
            self.result = self.operateFn(txnID, payload)
            self.error = ""
        except Exception as e:
            self.error = e.__doc__
            self.result = None
        return self.result
    def compensate(self, txnID: str, result):
        self.compensateFn(txnID, result)

class SagaResult(BaseModel):
    status: bool
    result: Any = None
    message: str


def frontend_recover(retry:int=3,interval=0, timeout = -1):
    def frontExec(executors: list[Tuple[
            Callable[[Any],SagaResult],
            Callable[[SagaResult],SagaResult]
        ]], payload) -> SagaResult:
        result = SagaResult(status=False,result=None,message='Not Task')
        for executor in executors:
            try_done, finish_done = executor
            result:SagaResult = try_done(payload)
            retries = retry
            while retries > 0 and not result.status:
                time.sleep(interval)
                retries -= 1
                result:SagaResult = try_done(payload)
            result:SagaResult = finish_done(result)
            if not result.status:
                break
            payload = result.result
        return result
    return frontExec


def sagaTask(operateFn, compensateFn) -> SagaTask:
    return SagaTask(operateFn,compensateFn)

def WithSaga(
        tasks:list[SagaTask],
        recoverFn:Callable[[list[Callable[[Any],SagaResult]],Any],SagaResult]
    ):
    def exec(payload):
        manager = SagaManager()
        executors:list[Tuple[
            Callable[[Any],SagaResult],
            Callable[[SagaResult],SagaResult]
        ]] = []
        for task in tasks:
            try_done,finish_done = manager.addTask(task)
            executors.append((try_done,finish_done))
        result:SagaResult = recoverFn(executors,payload)
        return result
    return exec

class SagaManager:
    def __init__(self):
        self._triedTasks: list[SagaTask] = []
        self._hasError: bool = False
        self._errmsg: str = ""
        self._txnID = uuid.uuid4()
    
    def addTask(self, task: SagaTask):
        def try_done(payload) -> SagaResult:
            if self._hasError:
                return SagaResult(
                    status=False,
                    result=None,
                    message="previous task is already error")
            result = task.operate(self._txnID, payload)
            return SagaResult(
                status=not task.error,
                result=result,
                message=task.error
            )
        def finish_done(result: SagaResult) -> SagaResult:
            self._triedTasks.append(task)
            if not result.status:
                self._hasError = True
                self._errmsg = result.message
            return result
        return try_done, finish_done
    
    def finalize(self):
        if self._hasError:
            for i in range(len(self._triedTasks)-1,0,-1):
                task = self._triedTasks[i]
                task.compensate(self._txnID, task.result)