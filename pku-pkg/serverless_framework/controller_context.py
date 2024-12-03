import os
import logging
from typing import Dict, List, Any, Callable, Tuple, Optional, Set
import time

from .funciton_placer import DeploymentGenerator
from .serverless_utils import TransportMode, Address
from .redis_db import RedisProxy

class ControllerContext:
    def __init__(self):
        pass
        
    def setup(self, transmode: TransportMode, profile: str, para: int, repeat: int, 
              ditto_placement: bool, launch: str, debug: bool, failure_tolerance: int,
              getoutputs: bool, remote_call_timeout: float, redis_wait_time: float,
              post_ratio: float, knative: bool, redis_yaml: str, redis_ip: str, 
              redis_port: int, redis_password: str, redis_preload_folder: str) -> None:
        self.transmode: TransportMode = TransportMode[transmode]
        self.profile_path: str = profile
        self.test_parallelism: int = para
        self.repeat: int = repeat
        self.ditto_placement: bool = ditto_placement
        self.launch_mode: str = launch
        self.debug: bool = debug
        self.failure_tolerance: int = failure_tolerance
        self.getoutputs: bool = getoutputs
        self.remote_call_timeout: float = remote_call_timeout
        self.redis_wait_time: float = redis_wait_time
        self.post_ratio: float = post_ratio
        self.knative: bool = knative
        self.redis_yaml: str = redis_yaml
        self.redis_ip: str = redis_ip              # has to match redis_yaml
        self.redis_port: int = redis_port          # has to match redis_yaml
        self.redis_password: str = redis_password  # has to match redis_yaml
        self.redis_preload_folder: str = redis_preload_folder

        import yaml
        with open(self.profile_path, 'r') as f:
            self.profile = yaml.load(f, Loader=yaml.FullLoader)
            self.app_name: str = self.profile['app_name']
            self.stages: List[str] = list(self.profile['stage_profiles'].keys())

        self.deploy = DeploymentGenerator(self.profile_path, self.ditto_placement is False, knative = self.knative)
        self.schedule: Dict[str, Address] = self.deploy.getIngress()
        self.params: Dict[str, Dict[str, Any]] = self.profile.get('default_params')

        self.worker_yamls: Dict[str, str] = self.deploy.generate_kubernetes_yamls('.')

         # check sanity of args.redis_yaml
        assert(os.path.exists(self.redis_yaml))
        assert(self.redis_yaml.endswith('.yaml'))
        redis_docs = []
        # change redis yaml to match these config
        with open(self.redis_yaml, 'r') as f:
            redis_docs = list(yaml.load_all(f, Loader=yaml.FullLoader))
            redis_docs[0]['spec']['containers'][0]['args'] = ["--port", str(self.redis_port), "--requirepass", self.redis_password]
            redis_docs[1]['spec']['ports'][0]['port'] = self.redis_port
            redis_docs[1]['spec']['externalIPs'][0] = self.redis_ip
            redis_docs[2]['stringData']['redis-host'] = self.redis_ip
            redis_docs[2]['stringData']['redis-port'] = str(self.redis_port)
            redis_docs[2]['stringData']['redis-password'] = self.redis_password
        
        with open(self.redis_yaml, 'w') as f:
            yaml.dump_all(redis_docs, f, Dumper=yaml.Dumper)

        # self.workflow_reqgen = WorkflowReqGen()
        # self.workflow_reqgen.clearup()
        self.wait_redis_start_time = time.perf_counter()
        # self.workflow_reqgen.create_redis(self.redis_yaml)

        
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