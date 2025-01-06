# How to build

```bash 
python3 setup.py bdist_wheel
```

You can find `.whl` file under `./dist/`

# How to use

First, install the `.whl` file using `pip3`.

```bash
python3 -m serverless_framework.controller --repeat 3 --launch coldstart --transmode allRedis 
```
Use command like this on controller,
```bash
python3 -m serverless_framework.worker ./PCA/lambda_function.py lambda_handler --port __worker-port__ --parallelism __parallelism__ --cache_server_port __cache-server-port__ --debug
```
and use command like this on worker. For more usage please use 
```bash 
python3 -m serverless_framework.controller --help
```
or 
```bash
python3 -m serverless_framework.worker --help
```

# TODO list 

1. using enum for launch mode in controller context