from setuptools import setup, find_packages

setup(
    name='serverless-framework',
    version='0.0.3',
    author='pku',
    description='',
    install_requires=[
        "kubernetes==31.0.0",
        "PyYAML==6.0.2",
        "redis==5.0.8"
    ],
    packages=find_packages(),
    python_requires='>=3.10',
    include_package_data=True,
    package_data= {
        'serverless_framework': ['*.yaml']
    }
)