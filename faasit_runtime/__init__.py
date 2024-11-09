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
    match containerConf['provider']:
        case 'local':
            def local_function(event,metadata:FaasitRuntimeMetadata = None) -> FaasitResult:
                LocalRuntime = load_runtime('local')
                frt = LocalRuntime(event,metadata)
                return fn(frt)
            return local_function
        case 'aliyun':
            def aliyun_function(arg0, arg1):
                AliyunRuntime = load_runtime('aliyun')
                frt = AliyunRuntime(arg0, arg1)
                return fn(frt)
            return aliyun_function
        case 'knative':
            def kn_function(event) -> FaasitResult:
                KnativeRuntime = load_runtime('knative')
                frt = KnativeRuntime(event)
                return fn(frt)
            return kn_function
        case 'aws':
            frt = FaasitRuntime(containerConf)
        case 'local-once':
            def localonce_function(event, 
                               workflow_runner = None,
                               metadata: FaasitRuntimeMetadata = None
                               ):
                LocalOnceRuntime = load_runtime('local-once')
                frt = LocalOnceRuntime(event, workflow_runner, metadata)
                result = fn(frt)
                return result
            return localonce_function
        case 'pku':
            def pku_function(md):
                PKURuntime = load_runtime('pku')
                frt = PKURuntime(md)
                return fn(frt)
            return pku_function
        case _:
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
        match container_conf['provider']:
            case 'local-once':
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
        match container_conf['provider']:
            case 'local':
                async def handler(event:dict, metadata=None):...
                    # metadata = createFaasitRuntimeMetadata(container_conf['funcName']) if metadata == None else metadata
                    # return await runner.run(event, metadata)
                return handler
            case 'aliyun':
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
            case 'local-once':
                def handler(event: dict):
                    LocalOnceRuntime = load_runtime('local-once')
                    frt = LocalOnceRuntime(event, RouteRunner(workflow.route), createFaasitRuntimeMetadata('workflow'))
                    workflow.setRuntime(frt)
                    result = workflow.execute(event)
                    return result
                return handler
            case 'pku':
                from serverless_framework.serverless_utils import TransportMode, Address, LockPair
                from serverless_framework.deployment import DeploymentGenerator
                from serverless_framework.redis_db import RedisProxy
                from serverless_framework.metadata import Metadata
                from kubernetes import config, client, utils
                from serverless_framework.engine import Engine
                import yaml,os,time,logging, threading
                def clearup(self):
                    os.system('kubectl delete ksvc --all --grace-period=3')
                    os.system('kubectl delete service --all --grace-period=3')
                    os.system('kubectl delete pods redis-pod --grace-period=3')
                    os.system('kubectl delete secret spilot-redis-config --grace-period=3')
                def handler(
                    transmode: str = 'auto',
                    profile_path: str = 'config/mlplpe.yaml',
                    repeat: int = 1,
                    para: int = 1,
                    ditto_placement: bool = False,
                    launch: str = 'tradition',
                    redis_preload_folder: str = 'Redis/preload/mlpipe',
                    debug = False,
                    failure_tolerance: int = 100,
                    getoutputs = False,
                    remote_call_timeout: float = 1.0,
                    redis_wait_time: float = 10.0,
                    post_ratio: float = 0.0,
                    knative = False    
                ):
                    transmode: TransportMode = TransportMode[transmode]
                    with open(profile_path) as f:
                        profile = yaml.load(f, Loader=yaml.FullLoader)
                        app_name = profile['app_name']
                        stages : List[str] = list(profile['stage_profiles'].keys())
                    deploy = DeploymentGenerator(profile_path, ditto_placement is False, knative)
                    schedule : Dict[str,Address] = deploy.getIngress()
                    params: Dict[str, Dict[str, Any]] = profile.get('default_params')
                    worker_yamls: Dict[str,str] = deploy.generate_kubernetes_yamls('.')

                    redis_yaml = 'Redis/redis.yaml'
                    redis_ip: str = "10.0.0.100"           # has to match redis_yaml
                    redis_port: int = 6889                 # has to match redis_yaml
                    redis_password: str = '293r9vfT7dfa&^' # has to match redis_yaml

                    clearup()
                    wait_redis_start_time = time.perf_counter()
                    config.load_kube_config()
                    k8s_client = client.ApiClient()
                    utils.create_from_yaml(k8s_client, redis_yaml)

                    worker_start_point: Dict[str, float] = deploy.get_worker_start_point()
                    redis_proxy: Optional[RedisProxy] = None
                    if knative:
                        logging.info("Knative mode. None of our techniques will be used.")
                        ditto_placement = False
                        transmode = TransportMode.allRedis
                        launch_mode = 'coldstart'
                    
                    assert(transmode in [TransportMode.allRedis, TransportMode.allTCP, TransportMode.auto])
                    assert(launch_mode in ['tradition', 'coldstart', 'prewarm'])
                    assert(all([stage in params for stage in stages]))
                    engines: List[Engine] = []
                    is_worker_launched: LockPair = LockPair(threading.Lock(), 
                                        {stage: False for stage in stages})
                    cmd = 'kubectl delete -f ' + ','.join(worker_yamls.values()) + '  --grace-period=0'
                    logging.info(f"Deleting all the workers with command: {cmd}")
                    os.system(cmd)
                    time.sleep(3)

                    logging.info("Initializing redis and preloading data...")

                    time_to_sleep = redis_wait_time - (time.perf_counter() - wait_redis_start_time)
                    if time_to_sleep > 0:
                        time.sleep(time_to_sleep)

                    # clear up redis and preload data into redis
                    redis_proxy = RedisProxy(host=redis_ip, port=redis_port, password=redis_password)
                    redis_proxy.clear()

                    for f in os.listdir(redis_preload_folder):
                        with open(os.path.join(redis_preload_folder, f), 'rb') as f:
                            redis_proxy.put(f.name.split('/')[-1], f.read())

                    if launch_mode == 'tradition':
                        logging.info("Traditional mode. Launching all workers...")
                        os.system(f"kubectl apply -f {','.join([worker_yamls[stage] for stage in stages])}")
                        for stage in stages:
                            is_worker_launched.value[stage] = True
                        time.sleep(10)

                    for i in range(para):
                        mds: Dict[str, Metadata] = {
                            stage: Metadata(f"{app_name}-{i}", stage, schedule, 
                                            transmode, params[stage], 
                                            redis_proxy, remote_call_timeout, post_ratio=post_ratio) for stage in stages}
                        
                        def sender_gen(stage: str, mds = mds) -> Callable[[], Metadata]:
                            def sender() -> Metadata:
                                if launch_mode == 'coldstart':
                                    is_worker_launched.lock.acquire()
                                    if not is_worker_launched.value[stage]:
                                        os.system(f"kubectl apply -f {worker_yamls[stage]}")
                                        is_worker_launched.value[stage] = True
                                    is_worker_launched.lock.release()

                                mds[stage].remote_call()
                                return mds[stage]
                            return sender
                        

                        funcs: Dict[str, Callable[[], Metadata]] = {
                            stage: sender_gen(stage) for stage in stages
                        }

                        def wrapper(s: str) -> None:
                            is_worker_launched.lock.acquire()
                            if not is_worker_launched.value[s]:
                                if os.system(f"kubectl apply -f {worker_yamls[s]}") != 0:
                                    raise Exception(f"Error: start worker {cmd} failed!")
                                is_worker_launched.value[s] = True
                            is_worker_launched.lock.release()


                        # TODO Refactor
                        engines.append(Engine(str(i), ctx.profile['DAG'], funcs, 
                                            timing_func=[] if launch_mode != 'prewarm' else [
                                                    (worker_start_point[stage], 
                                                        lambda s=stage: wrapper(s)
                                                    ) for stage in stages
                                                ],
                                            failure_tolerance=failure_tolerance,
                                            getoutputs=getoutputs))

                    overall_begin = time.time()

                    for e in engines: e.launch()
                    lats = [e.join() for e in engines]

                    overall_end = time.time()

                    logging.info("=============== Test finished! =================")
                    # log statistic
                    logging.info(f"Thput: {para / (overall_end - overall_begin)} RPS")

                    # print latency statistic, like minimum, avg, p0.5, p0.9, p0.99, p0.999, and p1.0
                    lats.sort()
                    logging.info(f"Latency (s): min={lats[0]}, avg={sum(lats) / len(lats)} "
                        f"p50={lats[int(len(lats) * 0.5)]}, p90={lats[int(len(lats) * 0.9)]} "
                        f"p99={lats[int(len(lats) * 0.99)]}, p999={lats[int(len(lats) * 0.999)]}, p100={lats[-1]}")
                        
                    return (overall_end - overall_begin, lats)

        return handler
    else: #type(fn) == type_Function:
        match container_conf['provider']:
            case 'aliyun':
                def handler(event: dict, *args):
                    return asyncio.run(fn_or_workflow(event, *args))
                return handler
            case _:
                def handler(event: dict, *args):
                    return fn_or_workflow(event, *args)
                return handler

__all__ = ["function","workflow","durable","create_handler"]