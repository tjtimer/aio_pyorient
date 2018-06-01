import asyncio

import pytest

from aio_pyorient.client import ODBClient
TEST_USER = 'test-user'
TEST_PASSWORD = 'test-pw'
TEST_DB = 'test-db'


@pytest.fixture(scope="function")
async def client(loop):
    async with ODBClient("localhost", 2424, loop=loop) as client:
        await client.connect(TEST_USER, TEST_PASSWORD)
        yield client

@pytest.fixture(scope="function")
async def db_client(loop):
    async with ODBClient("localhost", 2424, loop=loop) as client:
        await client.open_db(TEST_DB, TEST_USER, TEST_PASSWORD)
        yield client

@pytest.fixture(scope="function")
async def binary_db_client(loop):
    async with ODBClient(
            "localhost", 2424,
            serialization_type="ORecordSerializerBinary", loop=loop) as client:
        await client.open_db(TEST_DB, TEST_USER, TEST_PASSWORD)
        yield client
