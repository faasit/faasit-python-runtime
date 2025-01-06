clean:
	rm -f dist/serverless_framework-1.0.0-py3-none-any.whl

build: clean
	python3 setup.py bdist_wheel

install: build
	pip3 install dist/serverless_framework-1.0.0-py3-none-any.whl --force-reinstall