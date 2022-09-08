from setuptools import find_packages, setup



setup(
    name='pykeral',
    packages=find_packages(),
    version='0.1.0',
    description='A pandas to graph database library',
    setup_requires=['pandas','uuid'],
    author='Axrati',
    license='MIT',
)