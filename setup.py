from setuptools import setup

setup(
    name='sd-microservice',
    description='Projeto para matéria de Sistemas Distribuídos do curso de Ciência da Computação',
    author='Robert Broketa',
    install_requires=['fastapi', 'uvicorn'],
    packages=['app'],
    entry_points={
        'console_scripts': [
            'pratica-sd=app.main:main',
        ]
    }
)
