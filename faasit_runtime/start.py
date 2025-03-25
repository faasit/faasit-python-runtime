import os
import time
import argparse
import importlib.util
from faasit_runtime.utils.logging import log as logger
import sys
import mmap
from functools import wraps
import json
import multiprocessing

profile = 1
lock_file = "lock"
lock_string = "0"
ret_imm = False

mm_lock = None

def criu(handler):
    def wait():
        with open(lock_file, 'rb') as f:
            fd = f.fileno()
            mm_lock = mmap.mmap(fd, 0, flags=mmap.MAP_SHARED, prot=mmap.PROT_READ)
        while chr(mm_lock[0]) == lock_string:
            pass
    @wraps(handler)
    def wrapper(*args, **kwargs):
        wait()
        if not ret_imm:
            return handler(*args, **kwargs)
        os._exit(0)
    return wrapper

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='faasit fast start')
    parser.add_argument('--lambda_file', type=str, required=True, help='The lambda file to run')
    parser.add_argument('--function_name', type=str, required=True, help='The function name to run')
    parser.add_argument('--params', type=str, required=False, help='The parameters to pass to the lambda function', default='{}')
    args = parser.parse_args()
    lambda_file = args.lambda_file
    function_name = args.function_name
    # Load the user's lambda function
    try:
        spec = importlib.util.spec_from_file_location(function_name, lambda_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        func = getattr(module, function_name)
    except Exception as e:
        logger.error(f"Failed to load the lambda function: {e}")
        sys.exit(1)

    func = criu(func)
    try:
        params = json.loads(args.params)
    except:
        params = args.params
    
    # criu dump the func
    with open(lock_file, 'wb') as f:
        f.write(lock_string.encode())
    
    if os.path.exists("imgs"):
        os.system("rm -rf imgs")
    os.system("mkdir imgs")
    process = multiprocessing.Process(target=func, args=(params,))
    process.start()
    pid = process.pid
    print(f"pid: {pid}")
    os.system(f"criu dump --images-dir=./imgs -t {pid} -vvvv -o dump.log")
    os.chmod("imgs", 0o777)
    with open(lock_file, 'wb') as f:
        f.write(str(pid).encode())
    process.join()