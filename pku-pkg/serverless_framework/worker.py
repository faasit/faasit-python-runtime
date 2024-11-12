'''
This file is the entry point for the worker process. It is responsible for
- Loading the user's lambda function
- Starting the HTTP server
- Handling requests from the controller
- Handling cache-put requests from other workers
- Sending the result back to the controller no matter the result is success or failure

Any exception catched by the worker within the given lambda_handler will be 
treated as a failure.
'''


import sys
import importlib.util
import http.server
from socketserver import ThreadingMixIn
import pickle
from .metadata import Metadata
from .kv_cache import KVCache
from .redis_db import RedisProxy
import logging
import traceback
from typing import Callable, Optional, Dict, Any, Set
from concurrent.futures import ThreadPoolExecutor
from .serverless_utils import Result
import argparse
import logging
import cProfile
from .cache_server import CacheServer
from threading import Lock

lambda_file = None

def default_lambda_handler(md: Metadata) -> Any:
    print("default_lambda_handler: ", md.params)
    return None

lambda_handler: Callable[[Metadata], Any] = default_lambda_handler

# Read from secret tmpfs file.
try:
    with open('/etc/spilot-redis-config/redis-host','r') as f:
        redis_host = f.read()
    with open('/etc/spilot-redis-config/redis-port','r') as f:
        redis_port = int(f.read())
    with open('/etc/spilot-redis-config/redis-password','r') as f:
        redis_password = f.read()
except:
    logging.error("Failed to read redis password")
    logging.error(traceback.format_exc())
    sys.exit(1) 

kv_cache = KVCache()
redis_proxy = RedisProxy(host=redis_host, port=redis_port, password=redis_password)
thread_pool: ThreadPoolExecutor

def handler(identifier: str):
    md = request_buffer.pop(identifier)
    assert(md is not None)
    try:
        retval = lambda_handler(md)
    except Exception as e:
        logging.error(f"Error occurred while executing the lambda function {md.unique_execution_id}: {str(e)}")
        logging.error(traceback.format_exc())
        md.update_status(Result.Err((e, traceback.format_exc())))
    else:
        md.update_status(Result.Ok(retval))
        logging.info(f"Lambda function {md.id} executed successfully, queuing: {thread_pool._work_queue.qsize()} reqs. return value: {retval} ")


class RequestBuffer:
    def __init__(self):
        self.lock = Lock()
        self.queued_requests: Dict[str, Metadata] = {}

    def _get(self, identifier: str) -> Optional[Metadata]:
        with self.lock:
            return self.queued_requests.get(identifier, None)
        
    def _put(self, identifier: str, md: Metadata):
        with self.lock:
            self.queued_requests[identifier] = md

    def try_push(self, identifier: str, md: Metadata) -> bool:
        with self.lock:
            if identifier in self.queued_requests:
                # has previous
                if self.queued_requests[identifier].call_cnt < md.call_cnt:
                    self.queued_requests[identifier] = md
                    return True
                else:
                    return False
            else:
                # no previous
                self.queued_requests[identifier] = md       # the order of these two lines is important
                thread_pool.submit(handler, md.id)          # the order of these two lines is important
                return True
        
    def pop(self, identifier: str) -> Optional[Metadata]:
        with self.lock:
            return self.queued_requests.pop(identifier, None)


request_buffer: RequestBuffer = RequestBuffer()

class RequestHandler(http.server.BaseHTTPRequestHandler):
    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)

        try:
            data = pickle.loads(post_data)
            request_type = data['type']

            logging.debug(f"Received request: {request_type}, data: {data}")

            if (request_type == 'lambda-call'):
                metadata: Metadata = data.get('metadata')
                assert(metadata is not None)

                logging.debug(f"mode: {metadata.trans_mode}")
                logging.debug(f"schedule: {str(metadata.schedule)}")
                logging.debug(f"execution_namespace: {metadata.execution_namespace}")

                metadata.redis_proxy = redis_proxy
                metadata.worker_cache = kv_cache

                if request_buffer.try_push(metadata.id, metadata):
                    logging.info(f"Lambda function {metadata.unique_execution_id} is added into the queue, queuing: {thread_pool._work_queue.qsize()} reqs.")
                else:
                    logging.info(f"An older lambda call {metadata.unique_execution_id} is ignored. queuing: {thread_pool._work_queue.qsize()} reqs.")

                self.send_response(200)
                self.send_header('Content-type', 'application/octet-stream')
                self.end_headers()

            
            elif (request_type == 'cache-put'):
                key = data.get('key')
                value = data.get('value')
                if key is None or value is None:
                    self.send_error(400, 'Bad Request: Key or value not provided')
                    self.end_headers()
                    return
                
                logging.info(f"Received cache-put request: key={key}")
                kv_cache.put(key, value)

                self.send_response(200)
                self.send_header('Content-type', 'application/octet-stream')
                self.end_headers()

            elif request_type == 'cache-get':

                key = data.get('key')
                def cache_get_task(rh: RequestHandler, key: str):
                    logging.debug(f"handling cache-get request: key={key}")
                    if key is None:
                        rh.send_error(400, 'Bad Request: Key not provided')
                        return
                    bytes = kv_cache.get_bytes(key)
                    if bytes is None:
                        rh.send_error(404, 'Not Found: Key not found in the cache')
                    else:
                        rh.send_response(200)
                        rh.send_header('Content-type', 'application/octet-stream')
                        rh.end_headers()
                        rh.wfile.write(bytes)
                
                cache_get_task(self, key)
            elif request_type == 'cache-clear':
                prefix = data.get('prefix')
                if prefix is None:
                    self.send_error(400, 'Bad Request: Prefix not provided')
                    self.end_headers()
                    return
                cleared = kv_cache.clear_with_prefix(prefix)
                logging.debug(f"Cache cleared with prefix: {prefix}, {cleared} keys removed")
                self.send_response(200)
                self.send_header('Content-type', 'application/octet-stream')
                self.end_headers()
            else:
                self.send_error(400, f'Bad Request: Invalid request type: {request_type}')
                self.end_headers()
        except Exception as e:
            logging.error(f"Error occurred while processing the request: {str(e)}")
            logging.error(traceback.format_exc())
            self.send_error(500, str(e))


class HTTPServer(ThreadingMixIn, http.server.HTTPServer):
    pass

def start_server(port):
    server_address = ('', port)
    httpd = HTTPServer(server_address, RequestHandler)
    logging.info(f'Starting server on port {port}...')
    httpd.serve_forever()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Serverless Worker')
    parser.add_argument('module_file_path', type=str, help='Path to the module file')
    parser.add_argument('function_name', type=str, help='Name of the function')
    parser.add_argument('--port', type=int, help='Port number', required=True)
    parser.add_argument('--parallelism', type=int, help='Number of request to be handled in parallel', default=1)
    parser.add_argument('--debug', action='store_true', help='Enable debug mode', default=False)
    parser.add_argument('--profiling', action='store_true', help='Enable profiling mode', default=False)
    parser.add_argument('--cache_server_port', type=int, help='Port number for the cache server')
    # parser.add_argument('--ncall', type=int, help='Number of calls it can serve', default=1000000)
    args = parser.parse_args()
    
    profiler: Optional[cProfile.Profile] = None
    if args.profiling:
        profiler = cProfile.Profile()
        profiler.enable()

    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    module_file_path = args.module_file_path
    lambda_file = module_file_path
    function_name = args.function_name
    port = args.port
    parallelism = args.parallelism

    # Import the module and get the function
    try:
        # Load the module
        spec = importlib.util.spec_from_file_location(function_name, module_file_path)
        if spec is None:
            print(f"Error: Failed to load the module '{module_file_path}'")
            sys.exit(1)
        module = importlib.util.module_from_spec(spec)
        if spec.loader is None:
            print(f"Error: Failed to load the module '{module_file_path}'")
            sys.exit(1)
        spec.loader.exec_module(module)

        # Access the `function_name` in the module
        func = getattr(module, function_name)
        
        def functor (*args):
            return func(*args)
        lambda_handler = functor

    except Exception as e:
        print(f"Error: Failed to import the function '{function_name}' - {str(e)}")
        sys.exit(1)

    thread_pool = ThreadPoolExecutor(max_workers=parallelism)

    cache_server: Optional[CacheServer] = None

    try:
        if args.cache_server_port is not None:
            logging.debug(f'Starting cache server on port {args.cache_server_port}...')
            cache_server = CacheServer(kv_cache, args.cache_server_port)
            cache_server.start()
        start_server(port)
    finally:
        if args.cache_server_port is not None:
            assert(cache_server)
            logging.debug('Stopping cache server...')
            cache_server.stop()
        if args.profiling:
            assert(profiler)
            profiler.disable()
            profiler.dump_stats(f'./worker_{function_name}.prof')