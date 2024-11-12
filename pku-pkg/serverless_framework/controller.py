"""
Host controller for running workload for several times, measuring latency and thput.

The controller should be executed under the root directory of the project.

Do basic profile with '--para 1 --launch tradition' to fill times in the profile file. 
"""

import logging
import os
import threading
import time
from .serverless_utils import TransportMode, Address, LockPair
from typing import Dict, List, Any, Callable, Tuple, Optional, Set
from .metadata import Metadata
from .engine import Engine
from collections import namedtuple
from .deployment import DeploymentGenerator
from .redis_db import RedisProxy
from kubernetes import client, config, utils

class ControllerContext:
    def __init__(self):
        pass
    
    def clearup(self):
        os.system('kubectl delete ksvc --all --grace-period=3')
        os.system('kubectl delete service --all --grace-period=3')
        os.system('kubectl delete pods redis-pod --grace-period=3')
        os.system('kubectl delete secret spilot-redis-config --grace-period=3')

        
    def resolve_args_and_setup(self) -> None:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--transmode', type=str,        default='auto')
        parser.add_argument('--profile', type=str,          default='config/mlpipe.yaml')
        parser.add_argument('--repeat', type=int,           default=1)
        parser.add_argument('--para', type=int,             default=1)
        parser.add_argument('--ditto_placement', action='store_true', 
                                                            default=False)
        parser.add_argument('--launch', type=str,           default='tradition')
        parser.add_argument('--redis_preload_folder', type=str, 
                                                            default='Redis/preload/mlpipe')
        parser.add_argument('--debug', action='store_true', default=False)
        parser.add_argument('--failure_tolerance',type=int, default=100)
        parser.add_argument('--getoutputs',action='store_true',
                                                            default=False)
        parser.add_argument('--remote_call_timeout',type=float,
                                                            default=1.0)
        parser.add_argument('--redis_wait_time',type=float, default=10.0)
        parser.add_argument('--post_ratio',type=float,      default=0.0)
        parser.add_argument('--knative',action='store_true',default=False)
        args = parser.parse_args()

        self.transmode: TransportMode = TransportMode[args.transmode]
        self.profile_path: str = args.profile
        self.test_parallelism: int = args.para
        self.repeat: int = args.repeat
        self.ditto_placement: bool = args.ditto_placement
        self.launch_mode: str = args.launch
        self.debug: bool = args.debug
        self.failure_tolerance = args.failure_tolerance
        self.getoutputs = args.getoutputs
        self.remote_call_timeout = args.remote_call_timeout
        self.redis_wait_time = args.redis_wait_time
        self.post_ratio = args.post_ratio
        self.knative = args.knative

        logging.basicConfig(level=logging.DEBUG if self.debug else logging.INFO, 
                            format='%(asctime)s %(levelname)s %(message)s')

        import yaml
        with open(self.profile_path, 'r') as f:
            self.profile = yaml.load(f, Loader=yaml.FullLoader)
            self.app_name: str = self.profile['app_name']
            self.stages: List[str] = list(self.profile['stage_profiles'].keys())

        self.deploy = DeploymentGenerator(self.profile_path, self.ditto_placement is False, knative = self.knative)
        self.schedule: Dict[str, Address] = self.deploy.getIngress()
        self.params: Dict[str, Dict[str, Any]] = self.profile.get('default_params')

        self.worker_yamls: Dict[str, str] = self.deploy.generate_kubernetes_yamls('.')
        
        self.redis_yaml: str = 'Redis/redis.yaml'
        self.redis_ip: str = "10.0.0.100"           # has to match redis_yaml
        self.redis_port: int = 6889                 # has to match redis_yaml
        self.redis_password: str = '293r9vfT7dfa&^' # has to match redis_yaml

        self.clearup()
        self.wait_redis_start_time = time.perf_counter()
        config.load_kube_config()
        k8s_client = client.ApiClient()
        utils.create_from_yaml(k8s_client, self.redis_yaml)

        self.redis_preload_folder = args.redis_preload_folder
        self.worker_start_point: Dict[str, float] = self.deploy.get_worker_start_point()
        self.redis_proxy: Optional[RedisProxy] = None

        if self.knative:
            logging.info("Knative mode. None of our techniques is applied.")
            self.ditto_placement = False
            self.transmode = TransportMode.allRedis # We must use redis since one knative service only exposes one port.
            self.launch_mode = 'coldstart'

        # sanity check
        assert(self.transmode in [TransportMode.allRedis, TransportMode.allTCP, TransportMode.auto])
        assert(self.launch_mode in ['prewarm', 'coldstart', 'tradition'])



        # params sanity check
        assert(all([stage in self.params for stage in self.stages]))

    

ctx = ControllerContext()


def do_eval() -> Tuple[float, List[float]]:
    engines: List[Engine] = []
    is_worker_launched: LockPair = LockPair(threading.Lock(), 
                        {stage: False for stage in ctx.stages})
    cmd = 'kubectl delete -f ' + ','.join(ctx.worker_yamls.values()) + '  --grace-period=0'
    logging.info(f"Deleting all the workers with command: {cmd}")
    os.system(cmd)
    time.sleep(3)

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

    if ctx.launch_mode == 'tradition':
        logging.info("Traditional mode. Launching all workers...")
        os.system(f"kubectl apply -f {','.join([ctx.worker_yamls[stage] for stage in ctx.stages])}")
        for stage in ctx.stages:
            is_worker_launched.value[stage] = True
        time.sleep(10)

    for i in range(ctx.test_parallelism):
        mds: Dict[str, Metadata] = {
            stage: Metadata(f"{ctx.app_name}-{i}", stage, ctx.schedule, 
                            ctx.transmode, ctx.params[stage], 
                            ctx.redis_proxy, ctx.remote_call_timeout, post_ratio=ctx.post_ratio) for stage in ctx.stages}
        
        def sender_gen(stage: str, mds = mds) -> Callable[[], Metadata]:
            def sender() -> Metadata:
                if ctx.launch_mode == 'coldstart':
                    is_worker_launched.lock.acquire()
                    if not is_worker_launched.value[stage]:
                        os.system(f"kubectl apply -f {ctx.worker_yamls[stage]}")
                        is_worker_launched.value[stage] = True
                    is_worker_launched.lock.release()

                mds[stage].remote_call()
                return mds[stage]
            return sender
        

        funcs: Dict[str, Callable[[], Metadata]] = {
            stage: sender_gen(stage) for stage in ctx.stages
        }

        def wrapper(s: str) -> None:
            is_worker_launched.lock.acquire()
            if not is_worker_launched.value[s]:
                if os.system(f"kubectl apply -f {ctx.worker_yamls[s]}") != 0:
                    raise Exception(f"Error: start worker {cmd} failed!")
                is_worker_launched.value[s] = True
            is_worker_launched.lock.release()

        engines.append(Engine(str(i), ctx.profile['DAG'], funcs, 
                            timing_func=[] if ctx.launch_mode != 'prewarm' else [
                                    (ctx.worker_start_point[stage], 
                                        lambda s=stage: wrapper(s)
                                    ) for stage in ctx.stages
                                ],
                            failure_tolerance=ctx.failure_tolerance,
                            getoutputs=ctx.getoutputs))

    overall_begin = time.time()

    for e in engines: e.launch()
    lats = [e.join() for e in engines]

    overall_end = time.time()

    logging.info("=============== Test finished! =================")
    # log statistic
    logging.info(f"Thput: {ctx.test_parallelism / (overall_end - overall_begin)} RPS")

    # print latency statistic, like minimum, avg, p0.5, p0.9, p0.99, p0.999, and p1.0
    lats.sort()
    logging.info(f"Latency (s): min={lats[0]}, avg={sum(lats) / len(lats)} "
        f"p50={lats[int(len(lats) * 0.5)]}, p90={lats[int(len(lats) * 0.9)]} "
        f"p99={lats[int(len(lats) * 0.99)]}, p999={lats[int(len(lats) * 0.999)]}, p100={lats[-1]}")
        
    return (overall_end - overall_begin, lats)


def main():
    ctx.resolve_args_and_setup()    
    
    w_over_all = 0
    lats = []
    for i in range(ctx.repeat):
        logging.info(f"================= Test {i} =================")

        duration, this_lats = do_eval()
        w_over_all += duration
        lats.extend(this_lats)
    
    logging.info("=============== Overall Test finished! =================")
    logging.info(f"Repeat {ctx.repeat} times, each time {ctx.test_parallelism} requests.")
    # log statistic
    logging.info(f"Overall thput: {ctx.test_parallelism * ctx.repeat / w_over_all} RPS")

    # print latency statistic, like minimum, avg, p0.5, p0.9, p0.99, p0.999, and p1.0
    lats.sort()
    logging.info(f"Overall latency (s): min={lats[0]}, avg={sum(lats) / len(lats)} "
        f"p50={lats[int(len(lats) * 0.5)]}, p90={lats[int(len(lats) * 0.9)]} "
        f"p99={lats[int(len(lats) * 0.99)]}, p999={lats[int(len(lats) * 0.999)]}, p100={lats[-1]}")

if __name__ == "__main__":
    main()