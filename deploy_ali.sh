pip install wheel
python setup.py bdist_wheel
mkdir -p tmp/python
pip install dist/faasit_runtime-0.0.1-py3-none-any.whl -t tmp/python
s cli fc layer publish --code ./tmp --compatible-runtime python3.10,python3.11 --region cn-hangzhou --layer-name ft-rt-py