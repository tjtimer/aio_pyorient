"""

 test_server
"""
from aio_pyorient.handler import server
from tests.test_settings import TEST_PASSWORD, TEST_USER


async def test_connect(client):
    handler = server.ServerConnect(client, TEST_USER, TEST_PASSWORD)
    await handler.send()
    await handler.read()
    assert client._session_id > 0
    assert client._auth_token not in (b'', None)
    assert handler.done
