import kubernetes.client as k8s_client
import kubernetes.config as k8s_config
import kubernetes.utils as k8s_utils
import logging
import os
import time
from typing import List, Dict, Callable, Tuple
import threading

from .controller_context import ControllerContext
from .redis_db import RedisProxy
from .engine import Engine
from .serverless_utils import LockPair
from .controller_metadata import ControllerMetadata

class WorkflowReqHandler:
    def __init__(self, stages: List[str]):
        k8s_config.load_kube_config()
        self.client_core = k8s_client.CoreV1Api()
        self.engines: List[Engine] = []
        self.is_worker_launched: LockPair = LockPair(threading.Lock(), {stage: False for stage in stages})
        self.stages = stages
    
    def delete_all_service(self, grace_period_seconds: int = 0):
        services = self.client_core.list_service_for_all_namespaces(watch=False)
        for service in services.items:
            if service.metadata.namespace == 'default':
                logging.info(f"Deleting service {service.metadata.name} in namespace {service.metadata.namespace}")
                try:
                    self.client_core.delete_namespaced_service(
                        name=service.metadata.name,
                        namespace=service.metadata.namespace,
                        body=k8s_client.V1DeleteOptions(
                            grace_period_seconds=grace_period_seconds
                        )
                    )
                except k8s_client.exceptions.ApiException as e:
                    logging.error(f"Failed to delete service {service.metadata.name}: {e}")
                    continue
    
    def delete_pods_by_name(self, pods_name, grace_period_seconds: int = 0) -> bool:
        try:
            self.client_core.delete_namespaced_pod(
                name=pods_name,
                namespace='default',
                body=k8s_client.V1DeleteOptions(
                    grace_period_seconds=grace_period_seconds
                )
            )
            logging.info(f"Deleted pod {pods_name}")
            return True
        except k8s_client.exceptions.ApiException as e:
            logging.error(f"Failed to delete pod {pods_name}: {e}")
            return False

    def delete_secret_by_name(self, secret_name, grace_period_seconds: int = 0) -> bool:
        try:
            self.client_core.delete_namespaced_secret(
                name=secret_name,
                namespace='default',
                body=k8s_client.V1DeleteOptions(
                    grace_period_seconds=grace_period_seconds
                )
            )
            logging.info(f"Deleted secret {secret_name}")
            return True
        except k8s_client.exceptions.ApiException as e:
            logging.error(f"Failed to delete secret {secret_name}: {e}")
            return False
    
    def delete_service_by_name(self, service_name, grace_period_seconds: int = 0) -> bool:
        try:
            self.client_core.delete_namespaced_service(
                name=service_name,
                namespace='default',
                body=k8s_client.V1DeleteOptions(
                    grace_period_seconds=grace_period_seconds
                )
            )
            logging.info(f"Deleted service {service_name}")
            return True
        except k8s_client.exceptions.ApiException as e:
            logging.error(f"Failed to delete service {service_name}: {e}")
            return False
    
    def create_redis(self, redis_yaml: str):
        self._k8s_apply(redis_yaml)
        logging.info("Redis created.")

    def remove_previous_workflow(self, workflow_yamls: List[str]):
        cmd = 'kubectl delete -f ' + ','.join(workflow_yamls) + '  --grace-period=0'
        logging.info(f"Deleting all the workers with command: {cmd}")
        os.system(cmd)
        time.sleep(3)

    def initialize(self, ctx: ControllerContext):
        logging.info("Initializing engin and stage status...")
        self.engines= []
        self.is_worker_launched = LockPair(threading.Lock(), {stage: False for stage in self.stages})

        logging.info("Initializing redis and preloading data...")
        time_to_sleep = ctx.redis_wait_time - (time.perf_counter() - ctx.wait_redis_start_time)
        if time_to_sleep > 0:
            time.sleep(time_to_sleep)

        # clear up redis and preload data into redis
        ctx.redis_proxy = RedisProxy(host=ctx.redis_ip, port=ctx.redis_port, password=ctx.redis_password)
        ctx.redis_proxy.clear()

        for f in os.listdir(ctx.redis_preload_folder):
            with open(os.path.join(ctx.redis_preload_folder, f), 'rb') as f:
                ctx.redis_proxy.put(f.name.split('/')[-1], f.read())
    
    def _k8s_apply(self, worker_yaml: str):
        logging.info(f"Applying worker from {worker_yaml}")
        import yaml
        with open(worker_yaml, 'r') as f:
            bodies = yaml.load_all(f, Loader=yaml.FullLoader)
            for body in bodies:
                kind = body['kind']
                if kind == 'Service':
                    try:
                        self.client_core.create_namespaced_service(body=body, namespace='default')
                    except k8s_client.exceptions.ApiException as e:
                        self.delete_service_by_name(body['metadata']['name'], grace_period_seconds=3)
                        time.sleep(3)
                        self.client_core.create_namespaced_service(body=body, namespace='default')

                elif kind == 'Pod':
                    try:
                        self.client_core.create_namespaced_pod(body=body, namespace='default')
                    except k8s_client.exceptions.ApiException as e:
                        self.delete_pods_by_name(body['metadata']['name'], grace_period_seconds=3)
                        time.sleep(3)
                        self.client_core.create_namespaced_pod(body=body, namespace='default')
                    
                elif kind == 'Secret':
                    try:
                        self.client_core.create_namespaced_secret(body=body, namespace='default')
                    except k8s_client.exceptions.ApiException as e:
                        self.delete_secret_by_name(body['metadata']['name'], grace_period_seconds=3)
                        time.sleep(3)
                        self.client_core.create_namespaced_secret(body=body, namespace='default')
                else:
                    raise Exception(f"Error: unknown kind {kind}")

    def create_workflow_tradition(self, worker_yamls: Dict[str, str]):
        logging.info("Traditional mode. Launching all workers...")
        for stage in self.stages:
            try:
                self._k8s_apply(worker_yamls[stage])
            except k8s_client.exceptions.ApiException as e:
                raise Exception(f"Error: Failed to start worker {worker_yamls[stage]}: {e}")
            self.is_worker_launched.value[stage] = True
        time.sleep(10)

    def excute_workflow(self, ctx: ControllerContext):
        for i in range(ctx.test_parallelism):
            mds: Dict[str, ControllerMetadata] = {
                stage: ControllerMetadata(f"{ctx.app_name}-{i}", stage, ctx.schedule, 
                                ctx.transmode, ctx.params[stage], 
                                ctx.redis_proxy, ctx.remote_call_timeout, post_ratio=ctx.post_ratio) for stage in ctx.stages}
            
            def sender_gen(stage: str, mds = mds) -> Callable[[], ControllerMetadata]:
                def sender() -> ControllerMetadata:
                    if ctx.launch_mode == 'coldstart':
                        self.is_worker_launched.lock.acquire()
                        if not self.is_worker_launched.value[stage]:
                            self._k8s_apply(ctx.worker_yamls[stage])
                            self.is_worker_launched.value[stage] = True
                        self.is_worker_launched.lock.release()

                    mds[stage].remote_call()
                    return mds[stage]
                return sender
            

            funcs: Dict[str, Callable[[], ControllerMetadata]] = {
                stage: sender_gen(stage) for stage in ctx.stages
            }

            def wrapper(s: str) -> None:
                self.is_worker_launched.lock.acquire()
                if not self.is_worker_launched.value[s]:
                    # if os.system(f"kubectl apply -f {ctx.worker_yamls[s]}") != 0:
                        # raise Exception(f"Error: start worker {cmd} failed!")
                    try:
                        self._k8s_apply(ctx.worker_yamls[s])
                    except k8s_client.exceptions.ApiException as e:
                        raise Exception(f"Error: Failed to start worker {ctx.worker_yamls[s]}: {e}")
                    self.is_worker_launched.value[s] = True
                self.is_worker_launched.lock.release()

            self.engines.append(Engine(str(i), ctx.profile['DAG'], funcs, 
                                timing_func=[] if ctx.launch_mode != 'prewarm' else [
                                        (ctx.worker_start_point[stage], 
                                            lambda s=stage: wrapper(s)
                                        ) for stage in ctx.stages
                                    ],
                                failure_tolerance=ctx.failure_tolerance,
                                getoutputs=ctx.getoutputs))
    
    def get_results(self) -> Tuple[float, List[float]]:
        overall_begin = time.time()
        for e in self.engines: e.launch()
        lats = [e.join() for e in self.engines]
        overall_end = time.time()
        return overall_end - overall_begin, lats
