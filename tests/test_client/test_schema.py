"""
test_global_props
"""
from pprint import pprint

async def test_create_db(client):
    pprint(vars(client))
    await client.create_db('test-database')
    pprint(vars(client))
    assert client.is_ready


async def test_db_exist(client):
    pprint(vars(client))
    exist = await client.db_exist('test-database')
    print('exist: ', exist)
    pprint(vars(client))
    assert client.is_ready
