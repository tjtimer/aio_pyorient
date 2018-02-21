from setuptools import setup

setup(
    name='aio_pyorient',
    version='0.0.1',
    url='https://github.com/tjtimer/aio_pyorient',
    author='Tim "tjtimer" Jedro',
    author_email='tjtimer@gmail.com',
    description='async ODBClient client library',
    long_description=open('README.rst').read(),
    license='LICENSE',
    packages = [
        'aio_pyorient',
        'aio_pyorient.messages',
        'aio_pyorient.ogm',
    ],
    install_requires=[
        'pytest',
        'pytest-aiohttp',
        'pyorient_native'
    ]
)
