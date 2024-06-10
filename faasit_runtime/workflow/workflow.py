from typing import Callable,Dict,TYPE_CHECKING
from faasit_runtime.utils import get_function_container_config
from faasit_runtime.workflow.dag import DAG, ControlNode,DataNode
from faasit_runtime.workflow.ld import Lambda

from faasit_runtime.workflow.route import Route,RouteRunner

import asyncio
import inspect
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
    def __init__(self,route:Route = None) -> None:
        self.route = route
        self.params:Dict[str,Lambda] = {}
        self.dag = DAG()
        pass


    def invokeHelper(self,fn_name):
        runner = RouteRunner(self.route)
        container_conf = get_function_container_config()
        match container_conf['provider']:
            case 'local-once':
                handler = runner.route(fn_name)
                def local_once_invoke(event:Dict):
                    nonlocal runner
                    if inspect.iscoroutinefunction(handler):
                        return asyncio.run(handler(event,runner))
                    else:
                        return handler(event,runner)

                return local_once_invoke
            case _:
                raise ValueError(f"provider {container_conf['provider']} not supported")
    
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
            if inspect.iscoroutinefunction(fn):
                return asyncio.run(fn(*args,**kwargs))
            else:
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
        self.dag.add_node(result_node)
        return r

    def call(self, fn_name:str, fn_params:Dict[str,Lambda]) -> Lambda:
        invoke_fn = self.invokeHelper(fn_name)
        fn_ctl_node = ControlNode(invoke_fn)
        self.dag.add_node(fn_ctl_node)
        for key, ld in fn_params.items():
            self.build_function_param_dag(fn_ctl_node,key,ld)

        r = self.build_function_return_dag(fn_ctl_node)
        return r

    def func(self,fn,*args,**kwargs) -> Lambda:
        fn_ctl_node = ControlNode(Workflow.funcHelper(fn))
        self.dag.add_node(fn_ctl_node)
        for index,ld in enumerate(args):
            self.build_function_param_dag(fn_ctl_node,index,ld)
        for key, ld in kwargs.items():
            self.build_function_param_dag(fn_ctl_node,key,ld)

        r = self.build_function_return_dag(fn_ctl_node)
        return r
    
    async def exec(self,fn_name:str, event):
        runner = RouteRunner(self.route)
        container_conf = get_function_container_config()
        match container_conf['provider']:
            case 'local-once':
                handler = runner.route(fn_name)
                if inspect.iscoroutinefunction(handler):
                    return await handler(event)
                else:
                    return handler(event)
            case _:
                raise ValueError(f"provider {container_conf['provider']} not supported")
    
    
    
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
        end_node.is_end_node = True
    
    def __str__(self) -> str:
        return str(self.dag)
