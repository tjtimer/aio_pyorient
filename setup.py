from setuptools import setup

setup(name='aio_pyorient',
      version='0.0.1',
      author='Tim "tjtimer" Jedro <tjtimer@gmail.com>',
      description='asyncio OrientDB native client library',
      long_description=open('README.rst').read(),
      license='LICENSE',
      packages = [
          'aio_pyorient',
          'aio_pyorient.messages',
          'aio_pyorient.ogm',
    ]
      )
