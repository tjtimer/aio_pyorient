import pytest

from aio_pyorient import ODBClient
from .test_settings import TEST_DB, TEST_DB_PASSWORD, TEST_USER


@pytest.fixture(scope="function")
async def client(loop):
    async with ODBClient("localhost", 2424, loop=loop) as client:
        yield client

@pytest.fixture(scope="function")
async def db_client(loop):
    async with ODBClient("localhost", 2424, loop=loop) as client:
        await client.open_db(TEST_DB, TEST_USER, TEST_DB_PASSWORD)
        yield client

@pytest.fixture(scope="function")
async def binary_db_client(loop):
    async with ODBClient(
            "localhost", 2424,
            serialization_type="ORecordSerializerBinary", loop=loop) as client:
        await client.open_db(TEST_DB, TEST_USER, TEST_DB_PASSWORD)
        yield client
