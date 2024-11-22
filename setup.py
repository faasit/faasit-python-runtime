from setuptools import setup, find_packages

setup(
    name='faasit-runtime',
    version='0.0.1',
    author='dydy',
    description='Faasit Runtime support for Python',
    install_requires=[
        "pydantic==1.10.8",
        "python-dotenv==1.0.1",
        'serverless-framework'
    ],
    packages=find_packages(),
    python_requires='>=3.10',
)