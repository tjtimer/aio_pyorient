from aio_pyorient.handler import db
from aio_pyorient.handler.response_types import OpenDbResponse
from aio_pyorient.utils import ODBSignalPayload
from tests.test_settings import TEST_DB_PASSWORD, TEST_USER, TEST_DB


payloads = []
i = 0

async def test_db_open(client):
    global payloads
    global i
    _ows_ = '_ows_'
    _odr_ = '_odr_'
    async def increment_i(payload):
        assert isinstance(payload, ODBSignalPayload)
        global payloads
        global i
        payloads.append(payload)
        i += 1
        return i, payloads

    handler = db.OpenDb(client,
                        TEST_DB, TEST_USER, TEST_DB_PASSWORD,
                        ows_extra_payload=_ows_,
                        odr_extra_payload=_odr_
                        )
    handler.on_will_send(increment_i)
    handler.on_did_send(increment_i)
    handler.on_will_read(increment_i)
    handler.on_did_read(increment_i)
    assert handler._sent.is_set() is False
    assert handler.done is False
    assert len(payloads) is 0
    assert i is 0
    await handler.send()
    assert len(payloads) is 2
    assert i is 2
    assert payloads[0].extra == '_ows_'
    assert payloads[1].extra == None
    assert handler._sent.is_set() is True
    assert handler.done is False
    await handler.read()
    assert len(payloads) is 4
    assert i is 4
    assert payloads[2].extra == None
    assert payloads[3].extra == '_odr_'
    for payload in payloads:
        assert isinstance(payload.sender, handler.__class__)
    assert handler.done is True
    assert client.session_id is not -1
    assert client.auth_token not in (b'', None)
    assert client.db_opened == TEST_DB
    assert len(client.clusters) > 0
    for cluster in client.clusters:
        assert cluster.id >= 0
        assert isinstance(cluster.name, str)
        assert cluster.name != ""
    assert client.clusters.get('internal') == 0
    assert client.clusters.get(0) == 'internal'
    assert client.server_version is not None
