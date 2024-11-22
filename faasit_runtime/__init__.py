from .durable import durable

from faasit_runtime.runtime import (
    FaasitRuntime, 
    FaasitResult,
    createFaasitRuntimeMetadata,
    FaasitRuntimeMetadata,
    load_runtime
)
from faasit_runtime.utils import (
    get_function_container_config,
    callback,
)
from faasit_runtime.workflow import Workflow,Route,RouteBuilder,RouteRunner
from typing import Callable, Set, Any, List, Dict, Optional
import asyncio

type_Function = Callable[[Any], FaasitResult]

def transformfunction(fn: type_Function) -> type_Function:
    containerConf = get_function_container_config()
    provider = containerConf['provider']
    if provider == 'local':
        def local_function(event,metadata:FaasitRuntimeMetadata = None) -> FaasitResult:
            LocalRuntime = load_runtime('local')
            frt = LocalRuntime(event,metadata)
            return fn(frt)
        return local_function
    elif provider == 'aliyun':
        def aliyun_function(arg0, arg1):
            AliyunRuntime = load_runtime('aliyun')
            frt = AliyunRuntime(arg0, arg1)
            return fn(frt)
        return aliyun_function
    elif provider == 'knative':
        def kn_function(event) -> FaasitResult:
            KnativeRuntime = load_runtime('knative')
            frt = KnativeRuntime(event)
            return fn(frt)
        return kn_function
    elif provider == 'aws':
        frt = FaasitRuntime(containerConf)
    elif provider == 'local-once':
        def localonce_function(event, 
                            workflow_runner = None,
                            metadata: FaasitRuntimeMetadata = None
                            ):
            LocalOnceRuntime = load_runtime('local-once')
            frt = LocalOnceRuntime(event, workflow_runner, metadata)
            result = fn(frt)
            return result
        return localonce_function
    elif provider == 'pku':
        def pku_function(md):
            PKURuntime = load_runtime('pku')
            frt = PKURuntime(createFaasitRuntimeMetadata('stage'),md)
            return fn(frt)
        return pku_function
    else:
        raise ValueError(f"Invalid provider {containerConf['provider']}")

routeBuilder = RouteBuilder()

def function(*args, **kwargs):
    # Read Config for different runtimes
    if kwargs.get('name') is not None:
        fn_name = kwargs.get('name')
        def function(fn: type_Function) -> type_Function:
            new_func = transformfunction(fn)
            routeBuilder.func(fn_name).set_handler(new_func)
            return new_func

        return function
    else:
        fn = args[0]
        new_func = transformfunction(fn)
        routeBuilder.func(fn.__name__).set_handler(new_func)
        return new_func

def workflow(fn) -> Workflow:
    route = routeBuilder.build()
    def generate_workflow(
            workflow_runner: RouteRunner = None, 
            metadata = None) -> Workflow:
        wf = Workflow(route,fn.__name__)
        r  = fn(wf)
        wf.end_with(r)
        container_conf = get_function_container_config()
        if container_conf['provider'] == 'local-once':
            LocalOnceRuntime = load_runtime('local-once')
            frt = LocalOnceRuntime(
                None, 
                workflow_runner if workflow_runner else RouteRunner(route), 
                metadata if metadata else createFaasitRuntimeMetadata(fn.__name__)
            )
            wf.setRuntime(frt)
        return wf
    routeBuilder.workflow(fn.__name__).set_workflow(generate_workflow)
    return generate_workflow()
        

def create_handler(fn_or_workflow : type_Function | Workflow):
    container_conf = get_function_container_config()
    if isinstance(fn_or_workflow, Workflow):
        workflow = fn_or_workflow
        provider = container_conf['provider']
        if provider == 'local':
            async def handler(event:dict, metadata=None):...
                # metadata = createFaasitRuntimeMetadata(container_conf['funcName']) if metadata == None else metadata
                # return await runner.run(event, metadata)
            return handler
        elif provider == 'aliyun':
            def handler(args0, args1):
                if container_conf['funcName'] == '__executor':
                    AliyunRuntime = load_runtime('aliyun')
                    frt = AliyunRuntime(args0, args1)
                    workflow.setRuntime(frt)
                    return workflow.execute(frt.input())
                else:
                    fn = RouteRunner(workflow.route).route(container_conf['funcName'])
                    result = fn(args0,args1)
                    return result
            return handler
        elif provider == 'local-once':
            def handler(event: dict):
                LocalOnceRuntime = load_runtime('local-once')
                frt = LocalOnceRuntime(event, RouteRunner(workflow.route), createFaasitRuntimeMetadata('workflow'))
                workflow.setRuntime(frt)
                result = workflow.execute(event)
                return result
            return handler
        elif provider == 'pku':
            def handler():
                dag_json = workflow.validate()
                res = {}
                res['default_params'] = {}
                res['DAG'] = {}
                for stage, value in dag_json.items():
                    pre = value['pre']
                    params = value['params']
                    for para, para_val in params.items():
                        if para == 'params':
                            res['default_params'][stage] = para_val
                            break
                    res['DAG'][stage] = pre
                def invoke(profile_path:str,redis_preload_folder:str=None):
                    import sys
                    import os
                    from serverless_framework import controller
                    sys.argv = ['serverless_framework/controller.py',
                                '--repeat',
                                '2',
                                '--launch',
                                'tradition',
                                '--transmode',
                                'allTCP',
                                '--profile',
                                profile_path,
                            ]
                    if redis_preload_folder:
                        sys.argv.append('--redis_preload_folder')
                        sys.argv.append(redis_preload_folder)
                    controller.main()
                return res, invoke

        return handler
    else: #type(fn) == type_Function:
        provider = container_conf['provider']
        if provider == 'aliyun':
            def handler(event: dict, *args):
                return asyncio.run(fn_or_workflow(event, *args))
            return handler
        else:
            def handler(event: dict, *args):
                return fn_or_workflow(event, *args)
            return handler

__all__ = ["function","workflow","durable","create_handler"]