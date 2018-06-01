"""
test_global_props
"""
from pprint import pprint

from aio_pyorient.odb_types import ODBSchema


async def test_create_db(client):
    await client.create_db('test')
    assert client.is_ready


async def test_db_exist(client):
    exist = await client.db_exist('test')
    print('Db test exists: ', exist)
    assert client.is_ready
    assert exist is True

async def test_execute(db_client):
    print('test_execute')
    print('db_client')
    pprint(vars(db_client))
    schema = await ODBSchema(db_client).get()
    print('\nSCHEMA:')
    pprint(schema)
    assert schema is not None
