from typing import Callable,Dict,TYPE_CHECKING
from faasit_runtime.utils import get_function_container_config
from faasit_runtime.workflow.dag import DAG, ControlNode,DataNode
from faasit_runtime.workflow.ld import Lambda
from ..runtime import FaasitRuntime

from faasit_runtime.workflow.route import Route,RouteRunner
from ..api import invoke

class WorkflowInput:
    def __init__(self,workflow:"Workflow") -> None:
        self.workflow = workflow
        pass

    def get(self,key:str,default_val=None) -> Lambda:
        if self.workflow.params.get(key) != None:
            return self.workflow.params[key]
        else:
            if default_val != None:
                ld = Lambda(default_val)
            else:
                ld = Lambda()
            DataNode(ld)
            self.workflow.params[key] = ld
            return ld

class Workflow:
    def __init__(self,route:Route = None, name:str= None) -> None:
        self.route = route
        self.params:Dict[str,Lambda] = {}
        self.dag = DAG(self)
        self.frt: FaasitRuntime = None
        self.name: str = name
        pass

    def setRuntime(self, frt: FaasitRuntime):
        self.frt = frt
    def getRuntime(self):
        return self.frt


    def invokeHelper(self,fn_name):
        def invoke_fn(event:Dict):
            nonlocal self,fn_name
            return self.frt.call(fn_name, event)
        return invoke_fn
    @staticmethod
    def funcHelper(fn):
        def functionCall(data:dict):
            nonlocal fn
            args = []
            kwargs = {}
            for i in range(len(data)):
                if i not in data:
                    break
                args.append(data[i])
                data.pop(i)
            for key in data:
                kwargs[key] = data[key]
            return fn(*args,**kwargs)
        return functionCall

    def getEvent(self) -> WorkflowInput:
        return WorkflowInput(self)
    
    def build_function_param_dag(self,fn_ctl_node:ControlNode,key,ld:Lambda):
        if not isinstance(ld, Lambda):
            ld = Lambda(ld)
        param_node = DataNode(ld) if ld.getDataNode() == None else ld.getDataNode()
        param_node.add_succ_control_node(fn_ctl_node)
        fn_ctl_node.add_pre_data_node(param_node)
        fn_ctl_node.defParams(ld, key)
        self.dag.add_node(param_node)
        return param_node
    
    def build_function_return_dag(self,fn_ctl_node:ControlNode) -> Lambda:
        r = Lambda()
        result_node = DataNode(r)
        fn_ctl_node.set_data_node(result_node)
        result_node.set_pre_control_node(fn_ctl_node)
        self.dag.add_node(result_node)
        return r

    def call(self, fn_name:str, fn_params:Dict[str,Lambda]) -> Lambda:
        """
        for the remote code support
        """
        invoke_fn = self.invokeHelper(fn_name)
        fn_ctl_node = ControlNode(invoke_fn, fn_name)
        self.dag.add_node(fn_ctl_node)
        for key, ld in fn_params.items():
            self.build_function_param_dag(fn_ctl_node,key,ld)

        r = self.build_function_return_dag(fn_ctl_node)
        return self.catch(r)

    def func(self,fn,*args,**kwargs) -> Lambda:
        """
        for the local code support
        """
        fn_ctl_node = ControlNode(Workflow.funcHelper(fn))
        self.dag.add_node(fn_ctl_node)
        for index,ld in enumerate(args):
            self.build_function_param_dag(fn_ctl_node,index,ld)
        for key, ld in kwargs.items():
            self.build_function_param_dag(fn_ctl_node,key,ld)

        r = self.build_function_return_dag(fn_ctl_node)
        return self.catch(r)
    
    def catch(self, ld: Lambda) -> Lambda:
        """
        for the Lambda use map(etc) workflow support
        """
        return ld.becatch(self)
    
    def execute(self,event:dict):
        for key, ld in self.params.items():
            if event.get(key) != None:
                data_node = ld.getDataNode()
                data_node.set_value(event[key])
            elif ld.value != None:
                data_node = ld.getDataNode()
                data_node.set_value(ld.value)
            else:
                raise ValueError(f"missing parameter {key}")
        return self.dag.run()
    def end_with(self,ld:Lambda):
        if not isinstance(ld, Lambda):
            ld = Lambda(ld)
            end_node = DataNode(ld)
        else:
            end_node = ld.getDataNode()
        self.dag.add_node(end_node)
        end_node.is_end_node = True
    
    def validate(self):
        return self.dag.validate()
    def __str__(self) -> str:
        return str(self.dag)
