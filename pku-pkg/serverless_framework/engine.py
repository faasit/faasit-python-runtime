from .serverless_utils import TransportMode, Address, final_outputs_prefix
from typing import Callable, Dict, List, Any, Tuple
from threading import Thread
from .metadata import Metadata
import time
import logging

class Engine:
    def __init__(self, 
            name: str,
            dependencies: Dict[str, List[str]],
            exec_func: Dict[str, Callable[[], Metadata]],
            # for prewarm case that needs to launch containers in advance
            *,
            timing_func: List[Tuple[float, Callable[[], None]]] = [],
            executing_timeout: float = 1200,
            failure_tolerance: int = 100,
            getoutputs: bool = False,
        ):
        '''
        exec_func will be executed for many times until the last metadata it returns succeeds.
        '''

        self.name = name
        self.dependencies = dependencies
        self.exec_func = exec_func
        self.timing_func = timing_func
        self.failure_tolerance: int = failure_tolerance
        self.current_failure_cnt: int = 0
        self.getoutputs: bool = getoutputs
        self.retval: Dict[str, Any] = {}
        self.executing_timeout: float = executing_timeout

        self.start_time: float = 0
        self.finish_time: float = 0
        self.finished: bool = False # in case it dies early due to any unexpected exception

        
    def _execute(self) -> None:
        # stages that are executing (request acked by the worker)
        executing: Dict[str, Metadata] = {}
        success: Dict[str, Metadata] = {}

        if len(self.exec_func) == 0:
            logging.error(f"Engine{self.name}: no stages to execute.")
            raise Exception("No stages to execute.")
        logging.info(self.exec_func)
        while len(success) < len(self.exec_func):
            cur = time.time()

            for stage, metadata in executing.items():
                metadata.fetch_retval()

            # Refresh all executing stages
            failure = {stage: metadata for stage, metadata in executing.items() 
                if metadata.fail() or ((cur - metadata.call_time > self.executing_timeout) and metadata.working())}
            succ_delta = {stage: metadata for stage, metadata in executing.items()
                            if metadata.succ()}
            success.update(succ_delta)
            executing = {stage: metadata for stage, metadata in executing.items() 
                         if stage not in failure and stage not in succ_delta}

            if len(failure) > 0:
                logging.warning(f"Engine{self.name}: {list(failure.keys())} failed. Retrying...")
                logging.warning(f"Engine{self.name}: failure retval = { {metadata.stage: str(metadata.get_reply()) for metadata in failure.values() }}")

            if len(succ_delta) > 0:
                logging.info(f"Engine{self.name}: {list(succ_delta.keys())} succeeded.")
            self.current_failure_cnt += len(failure)
            if self.current_failure_cnt >= self.failure_tolerance:
                logging.error(f"Too many failures ({self.current_failure_cnt} vs tolerance {self.failure_tolerance}) in Engine{self.name}.")
                import os
                os._exit(1)

            # Find all stages that are ready to execute
            can_fire = [stage for stage in self.exec_func.keys() 
                     if stage not in success and stage not in executing 
                        and all(dep in success for dep in self.dependencies[stage])]
            
            if len(can_fire) > 0:
                logging.debug(f"Engine{self.name}: can_fire: " + str(can_fire))

            # Execute all ready stages
            for stage in can_fire:
                try:
                    md = self.exec_func[stage]()
                    executing[stage] = md
                except Exception as e:
                    logging.error(f"Engine{self.name}: {stage} launched failed with exception: {e}")
                    
            time.sleep(0.1)
            
        self.retval = {stage: metadata.get_reply() for stage, metadata in success.items()}
        self.finish_time = time.time()

        for md in success.values():
            md.cache_clear()

        try: 
            # Get any md from this dict.
            md: Metadata = next(iter(success.values()))
            proxy = md.redis_proxy
            assert(proxy)

            if self.getoutputs:
                proxy.dump_keys_by_prefix(md.namespace_obj_prefix() + final_outputs_prefix, '/tmp/outputs/' + md.execution_namespace)

            # Clear redis cache.
            proxy.delete_keys_by_prefix(md.namespace_obj_prefix())

        except Exception as e:
            logging.error(f"Engine{self.name}: get outputs and clear redis failed with exception: {e}")
            import traceback
            traceback.print_exc()

        logging.info(f"Engine{self.name}: finished! all retval={self.retval}")
        self.finished = True
        

    def launch(self) -> None:
        logging.info(f"Engine{self.name}: launching...")

        self.start_time = time.time()
        self.task = Thread(target=self._execute, args=())
        self.task.start()

        self.timer_task = []

        def executor(delta: float, func: Callable[[], None]):
            time.sleep(delta)
            func()

        for t in self.timing_func:
            logging.info(t)
            self.timer_task.append(Thread(target=executor, args=(t[0], t[1])))

        for t in self.timer_task:
            t.start()

    def join(self) -> float:
        logging.debug(f"Engine{self.name}: joining...")
        timeout = 3600
        self.task.join(timeout = timeout)
        if self.task.is_alive():
            logging.error(f"Engine{self.name}: joining timeout for {timeout}s. Exiting...")
            import os
            os._exit(1)

        for t in self.timer_task:
            t.join(timeout = timeout)
            if t.is_alive():
                logging.error(f"Engine{self.name}: timer task joining timeout for {timeout}s. Exiting...")
                import os
                os._exit(1)

        if not self.finished:
            logging.error(f"Engine{self.name}: not finished the execution. Exiting...")
            import os
            os._exit(1)

        return self.finish_time - self.start_time