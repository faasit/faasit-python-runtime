from typing import List, Tuple
import time

from .workflow_reqhandler import WorkflowReqHandler
from .controller_context import ControllerContext

class WorkflowReqGen:
    def __init__(self, stages: List[str]):
        self.handler = WorkflowReqHandler(stages)

    def clearup(self): 
        self.handler.delete_all_service(grace_period_seconds=3)
        self.handler.delete_pods_by_name('redis-pod', grace_period_seconds=3)
        self.handler.delete_secret_by_name('spilot-redis-config', grace_period_seconds=3)
        time.sleep(3)

    def create_redis(self, redis_yaml: str) -> Tuple[float, List[float]]:
        self.handler.create_redis(redis_yaml)

    def create_workflow(self, ctx: ControllerContext):
        self.handler.remove_previous_workflow(list(ctx.worker_yamls.values()))
        self.handler.initialize(ctx)
        if ctx.launch_mode == "tradition":
            self.handler.create_workflow_tradition(ctx.worker_yamls)
        self.handler.excute_workflow(ctx)
        return self.handler.get_results()