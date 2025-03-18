python setup.py bdist_wheel
sudo rm -rf ~/.pypi/faasit_runtime-1.0.0-py3-none-any.whl
twine upload --repository-url http://localhost:12121/ dist/* -u "faasit" -p "faasit-pypi" --verbose