from setuptools import find_packages, setup


# python setup.py bdist_wheel

setup(
    name='pykeral',
    packages=find_packages(),
    version='0.1.0',
    description='A pandas to graph database library',
    setup_requires=['pandas','uuid', 'numpy'],
    author='Axrati',
    license='MIT',
)


