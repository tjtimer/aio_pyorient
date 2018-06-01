from pprint import pprint

from aio_pyorient.message import db
from tests.conftest import TEST_DB, TEST_PASSWORD, TEST_USER


signals_received = 0

async def test_db_open(client):

    handler = db.OpenDb(client,
                        TEST_DB, TEST_USER, TEST_PASSWORD
                        )
    assert handler._sent.is_set() is False
    assert handler.done is False
    await handler.send()
    assert handler._sent.is_set() is True
    assert handler.done is False
    await handler.read()
    assert handler.done is True
    assert client.session_id is not -1
    assert client.auth_token not in (b'', None)
    assert client.db_opened == TEST_DB
    assert len(client.clusters) > 0
    for cluster in client.clusters:
        assert cluster.id >= 0
        assert isinstance(cluster.name, str)
        assert cluster.name != ""
    assert 0 in client.clusters.get('internal')
    assert 'internal' in client.clusters.get(0)
    assert client.server_version is not None
    pprint(vars(client))

async def test_db_size(db_client):
    size = await db_client.db_size()
    print(f'client: {vars(db_client)}')
    print(f'size: {size}')
    assert size > 0

async def test_db_record_count(db_client):
    rec_count = await db_client.db_record_count()
    print(f'client: {vars(db_client)}')
    print(f'rec_count: {rec_count}')
    assert rec_count > 0
