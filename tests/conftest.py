import asyncio

import pytest

from aio_pyorient import OrientDB

@pytest.fixture(scope="module")
def loop(request):
    loop = asyncio.get_event_loop()
    def stop():
        loop.stop()
        loop.close()
    request.addfinalizer(stop)
    return loop

@pytest.fixture(scope="module")
def client(loop):
    client = OrientDB("localhost", 2424, loop=loop)
    return client
