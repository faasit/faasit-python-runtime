from .workflow.route import Route,RouteRunner
from .utils import get_function_container_config

def invoke(route:Route,fn_name:str,event:dict) -> dict:
    runner = RouteRunner(route)
    container_conf = get_function_container_config()
    match container_conf['provider']:
        case 'local-once':
            handler = runner.route(fn_name)
            return handler(event,runner)
        case _:
            raise ValueError(f"provider {container_conf['provider']} not supported")