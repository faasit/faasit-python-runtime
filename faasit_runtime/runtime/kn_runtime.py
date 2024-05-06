from faasit_runtime.runtime.faasit_runtime import (
    FaasitRuntime, 
    InputType,
    CallResult
)
import requests
import os

class KnativeRuntime(FaasitRuntime):
    name: str = 'knative'
    def __init__(self,data) -> None:
        super().__init__()
        self._input = data
    def input(self):
        return self._input
    def output(self, _out):
        return _out
    async def call(self, fnName:str, fnParams: InputType) -> CallResult:
        wf_name = os.environ.get('FAASIT_WORKFLOW_NAME','')
        if wf_name != '':
            fnName = f"{wf_name}-{fnName}"
        resp = requests.post(f"http://{fnName}.default.10.0.0.233.sslip.io", json=fnParams, headers={'Content-Type': 'application/json'}, proxies={'http': None, 'https': None})
        return resp.json()
    
    def tell(self):
        pass