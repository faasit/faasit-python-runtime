from faasit_runtime.txn import sagaTask,WithSaga,frontend_recover
from faasit_runtime import FaasitRuntime,function,create_handler
import random

@function
def sagaTest(frt: FaasitRuntime):
    def operateFn(txnID, payload):
        print(f"operateFn {txnID} {payload}")
        flag = random.choice([True,False])
        print(flag)
        if flag == True:
            print("success")
            return payload
        else:
            print("random error")
            raise Exception("random error")
    def compensateFn(txnID, result):
        print(f"compensateFn {txnID} {result}")
    task1 = sagaTask(operateFn,compensateFn)
    task2 = sagaTask(operateFn,compensateFn)
    withSaga = WithSaga([task1,task2],frontend_recover(5,0.5))
    return frt.output((withSaga("test")))
handler = create_handler(sagaTest)