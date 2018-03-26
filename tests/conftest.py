import asyncio

import pytest

from aio_pyorient import ODBClient
from aio_pyorient.pool import ODBPool
from .test_settings import TEST_DB, TEST_DB_PASSWORD, TEST_USER

# asyncio.set_event_loop_policy(asyncio.AbstractEventLoopPolicy())

@pytest.fixture(scope="module")
def gloop():
    loop = asyncio.get_event_loop()
    asyncio.set_event_loop(loop=loop)
    yield loop
    loop.stop()
    loop.close()

@pytest.fixture(scope="function")
async def client(loop):
    async with ODBClient("localhost", 2424, loop=loop) as client:
        yield client

@pytest.fixture(scope="function")
async def db_client(loop):
    loop = asyncio.get_event_loop()
    async with ODBClient("localhost", 2424, loop=loop) as client:
        await client.open_db(TEST_DB, TEST_USER, TEST_DB_PASSWORD)
        yield client

@pytest.fixture(scope="function")
async def test_pool(loop):
    async with ODBPool(TEST_USER, TEST_DB_PASSWORD, db_name=TEST_DB, loop=loop) as pool:
        yield pool
