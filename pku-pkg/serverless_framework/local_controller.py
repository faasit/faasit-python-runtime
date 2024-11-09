"""
Launch and drive the workers locally, without Kubernetes deployment.
"""

import logging
import os
import threading
import time
from serverless_utils import TransportMode, Address, LockPair
from typing import Dict, List, Any, Callable, Tuple, Optional, Set
from metadata import Metadata
from engine import Engine
from collections import namedtuple
from deployment import DeploymentGenerator
from redis_db import RedisProxy


class ControllerContext:
    def __init__(self):
        pass

    def resolve_args(self) -> None:
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument('--transmode', type=str,        default='auto')
        parser.add_argument('--profile', type=str,          default='config/video-ana.yaml')
        parser.add_argument('--repeat', type=int,           default=1)
        parser.add_argument('--para', type=int,             default=1)
        parser.add_argument('--redis_preload_folder', type=str, 
                                                            default='Redis/preload/video-ana')
        parser.add_argument('--debug', action='store_true', default=False)
        args = parser.parse_args()

        self.transmode: TransportMode = TransportMode[args.transmode]
        self.profile_path: str = args.profile
        self.test_parallelism: int = args.para
        self.repeat: int = args.repeat
        self.debug: bool = args.debug

        logging.basicConfig(level=logging.DEBUG if self.debug else logging.INFO, 
                            format='%(asctime)s %(levelname)s %(message)s')

        import yaml
        with open(self.profile_path, 'r') as f:
            self.profile = yaml.load(f, Loader=yaml.SafeLoader)
            self.app_name: str = self.profile['app_name']
            self.stages: List[str] = list(self.profile['stage_profiles'].keys())

        self.deploy = DeploymentGenerator(self.profile_path, False, local_placement = True)
        self.schedule: Dict[str, Address] = self.deploy.get_placement_with_port()
        self.params: Dict[str, Dict[str, Any]] = self.profile.get('default_params')

        self.redis_preload_folder = args.redis_preload_folder
        self.redis_proxy: Optional[RedisProxy] = None

        # sanity check
        assert(self.transmode in [TransportMode.allRedis, TransportMode.allTCP, TransportMode.auto])

        # params sanity check
        assert(all([stage in self.params for stage in self.stages]))


ctx = ControllerContext()


def do_eval() -> List[float]:
    engines: List[Engine] = []
    logging.info("Initializing redis and preloading data...")

    # clear up redis and preload data into redis
    ctx.redis_proxy = RedisProxy()
    ctx.redis_proxy.clear()

    for f in os.listdir(ctx.redis_preload_folder):
        with open(os.path.join(ctx.redis_preload_folder, f), 'rb') as f:
            ctx.redis_proxy.put(f.name.split('/')[-1], f.read())

    logging.info("Local mode. Launching all workers...")
    cmds = ctx.deploy.get_worker_commandlines()

    for cmd in cmds.values():
        ret = os.system(cmd + ' &')
        if ret != 0:
            raise Exception(f"Failed to launch worker with command: {cmd}")

    for i in range(ctx.test_parallelism):
        mds: Dict[str, Metadata] = {
            stage: Metadata(f"{ctx.app_name}-{i}", stage, ctx.schedule, 
                            ctx.transmode, ctx.params[stage], 
                            ctx.redis_proxy) for stage in ctx.stages}
        
        def sender_gen(stage: str, mds = mds) -> Callable[[], Metadata]:
            def sender() -> Metadata:
                mds[stage].remote_call()
                return mds[stage]
            return sender
        

        funcs: Dict[str, Callable[[], Metadata]] = {
            stage: sender_gen(stage) for stage in ctx.stages
        }

        engines.append(Engine(str(i), ctx.profile['DAG'], funcs, [], 0))

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
        
    return lats


if __name__ == "__main__":
    ctx.resolve_args()

    w_overall_begin = time.time()
    lats = []
    for i in range(ctx.repeat):
        logging.info(f"================= Test {i} =================")
        lats += do_eval()
    w_overall_end = time.time()
    
    logging.info("=============== Overall Test finished! =================")
    logging.info(f"Repeat {ctx.repeat} times, each time {ctx.test_parallelism} requests.")
    # log statistic
    logging.info(f"Thput: {ctx.test_parallelism * ctx.repeat / (w_overall_end - w_overall_begin)} RPS")

    # print latency statistic, like minimum, avg, p0.5, p0.9, p0.99, p0.999, and p1.0
    lats.sort()
    logging.info(f"Latency (s): min={lats[0]}, avg={sum(lats) / len(lats)} "
        f"p50={lats[int(len(lats) * 0.5)]}, p90={lats[int(len(lats) * 0.9)]} "
        f"p99={lats[int(len(lats) * 0.99)]}, p999={lats[int(len(lats) * 0.999)]}, p100={lats[-1]}")
    
    