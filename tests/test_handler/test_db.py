from pprint import pprint

from aio_pyorient.handler import db
from aio_pyorient.utils import ODBSignalPayload
from tests.test_settings import TEST_DB, TEST_DB_PASSWORD, TEST_USER


signals_received = 0

async def test_db_open(client):
    async def increment_i(payload):
        assert isinstance(payload, ODBSignalPayload)
        global signals_received
        assert isinstance(payload.sender, db.OpenDb)
        if signals_received is 0:
            assert payload.extra == '_ows_'
        elif signals_received is 3:
            assert payload.extra == '_odr_'
        else:
            assert payload.extra is None
        signals_received += 1
        return signals_received

    handler = db.OpenDb(client,
                        TEST_DB, TEST_USER, TEST_DB_PASSWORD,
                        ows_extra_payload='_ows_',
                        odr_extra_payload='_odr_'
                        )
    handler.on_will_send(increment_i)
    handler.on_did_send(increment_i)
    handler.on_will_read(increment_i)
    handler.on_did_read(increment_i)
    assert handler._sent.is_set() is False
    assert handler.done is False
    assert signals_received is 0
    await handler.send()
    assert handler._sent.is_set() is True
    assert handler.done is False
    assert signals_received is 2  # will_send, did_send
    await handler.read()
    assert handler.done is True
    assert signals_received is 4  # will_send, did_send, will_read, did_read
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
    assert client.server_version not in (None, '')
    pprint(vars(client))
