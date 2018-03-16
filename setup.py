from setuptools import setup

setup(
    name='aio_pyorient',
    version='0.0.1',
    url='https://github.com/tjtimer/aio_pyorient',
    author='Tim "tjtimer" Jedro',
    author_email='tjtimer@gmail.com',
    description='async OrientDB client library for python',
    long_description=open('README.rst').read(),
    license='LICENSE',
    packages = [
        'aio_pyorient',
    ],
    install_requires=[
        'arrow',
        'pytest',
        'pytest-aiohttp'
    ]
)
