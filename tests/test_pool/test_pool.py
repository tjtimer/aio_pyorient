"""

 test_pool
"""
import asyncio
from pprint import pprint

from aio_pyorient.pool import ODBPool
from tests.test_settings import TEST_PASSWORD, TEST_USER, TEST_DB


async def test_pool(loop):
    async with ODBPool(TEST_USER, TEST_PASSWORD, db_name=TEST_DB, loop=loop) as pool:
        assert pool.min is 5
        assert pool.max == 30000
        assert not pool.is_full
        assert pool.available_clients is pool.min
        assert pool.is_ready
        client_1 = await pool.acquire()
        client_2 = await pool.acquire()
        client_3 = await pool.acquire()
        assert client_1.session_id >= 0
        assert client_1._auth_token not in (b'', '', None)
        assert client_1._sock.connected is True
        assert client_2.session_id >= 0
        assert client_2._auth_token not in (b'', '', None)
        assert client_2._sock.connected is True
        assert client_3.session_id >= 0
        assert client_3._auth_token not in (b'', '', None)
        assert client_3._sock.connected is True
        assert client_1.session_id != client_2.session_id
        assert client_1.session_id != client_3.session_id
        assert client_2.session_id != client_3.session_id
    assert pool.done
    assert pool.available_clients is 0
