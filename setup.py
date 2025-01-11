from setuptools import setup, find_packages

setup(
    name='faasit-runtime',
    version='1.0.0',
    author='dydy',
    description='Faasit Runtime support for Python',
    install_requires=[
        "pydantic==1.10.8",
        "python-dotenv==1.0.1"
    ],
    extras_require={
        "pku": [
            'serverless-framework==1.0.0',
        ]
    },
    packages=find_packages(),
    python_requires='>=3.10',
)