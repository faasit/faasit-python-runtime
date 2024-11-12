from setuptools import setup, find_packages

setup(
    name='serverless_framework',
    version='0.0.1',
    author='pku',
    description='',
    install_requires=[
        "kubernetes==31.0.0",
        "PyYAML==6.0.2",
    ],
    packages=find_packages(),
    python_requires='>=3.10',
)