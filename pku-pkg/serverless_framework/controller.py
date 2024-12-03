"""
Host controller for running workload for several times, measuring latency and thput.

The controller should be executed under the root directory of the project.

Do basic profile with '--para 1 --launch tradition' to fill times in the profile file. 
"""

import logging
import os
import threading
import time
from typing import Dict, List, Any, Callable, Tuple, Optional, Set

from .serverless_utils import TransportMode, Address, LockPair
from .controller_context import ControllerContext
from .controller_metadata import ControllerMetadata
from .engine import Engine
from .redis_db import RedisProxy

from .workflow_reqgen import WorkflowReqGen

ctx = ControllerContext()


def do_eval(workflow_reqgen: WorkflowReqGen) -> Tuple[float, List[float]]:
    duration, lats = workflow_reqgen.create_workflow(ctx)


    logging.info("=============== Test finished! =================")
    # log statistic
    logging.info(f"Thput: {ctx.test_parallelism / duration} RPS")

    # print latency statistic, like minimum, avg, p0.5, p0.9, p0.99, p0.999, and p1.0
    lats.sort()
    logging.info(f"Latency (s): min={lats[0]}, avg={sum(lats) / len(lats)} "
        f"p50={lats[int(len(lats) * 0.5)]}, p90={lats[int(len(lats) * 0.9)]} "
        f"p99={lats[int(len(lats) * 0.99)]}, p999={lats[int(len(lats) * 0.999)]}, p100={lats[-1]}")
        
    return (duration, lats)

def resolve_args():
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
                                                        default='')
    parser.add_argument('--debug', action='store_true', default=False)
    parser.add_argument('--failure_tolerance',type=int, default=100)
    parser.add_argument('--getoutputs',action='store_true',
                                                        default=False)
    parser.add_argument('--remote_call_timeout',type=float,
                                                        default=1.0)
    parser.add_argument('--redis_wait_time',type=float, default=10.0)
    parser.add_argument('--post_ratio',type=float,      default=0.0)
    parser.add_argument('--knative',action='store_true',default=False)

    parser.add_argument('--redis_yaml', type=str,       default=os.path.join(os.path.dirname(__file__),'redis.yaml'))
    parser.add_argument('--redis_ip', type=str,         default="10.0.0.100")
    parser.add_argument('--redis_port', type=int,       default=6379)
    parser.add_argument('--redis_password', type=str,   default="293r9vfT7dfa&^")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, 
                            format='%(asctime)s %(levelname)s %(message)s')

    ctx.setup(transmode=args.transmode, profile=args.profile, para=args.para, repeat=args.repeat, 
              ditto_placement=args.ditto_placement, launch=args.launch, debug=args.debug,
              failure_tolerance=args.failure_tolerance, getoutputs=args.getoutputs,
              remote_call_timeout=args.remote_call_timeout, redis_wait_time=args.redis_wait_time,
              post_ratio=args.post_ratio, knative=args.knative, redis_yaml=args.redis_yaml,
              redis_ip=args.redis_ip, redis_port=args.redis_port, redis_password=args.redis_password,
              redis_preload_folder=args.redis_preload_folder)

if __name__ == "__main__":
    resolve_args()
    workflow_reqgen = WorkflowReqGen(ctx.stages)
    workflow_reqgen.clearup()
    workflow_reqgen.create_redis(ctx.redis_yaml)
    
    w_over_all = 0
    lats = []
    for i in range(ctx.repeat):
        logging.info(f"================= Test {i} =================")

        duration, this_lats = do_eval(workflow_reqgen)
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
