"""

 test_pool
"""
from random import randint

from aio_pyorient.pool import ODBPool
from tests.test_settings import TEST_DB, TEST_PASSWORD, TEST_USER


async def test_pool(loop):
    cl_count = randint(1, 125)
    print(f'test will acquire {cl_count} clients')
    async with ODBPool(TEST_USER, TEST_PASSWORD, db_name=TEST_DB, loop=loop) as pool:
        assert pool.min is 5
        assert pool.max == 30000
        assert pool.available_clients is pool.min
        assert pool.is_ready
        all_clients = [await pool.acquire() for _ in range(cl_count)]
        assert all([cl.session_id >= 0 for cl in all_clients])
        assert all([cl._auth_token not in (b'', '', None) for cl in all_clients])
        assert all([cl._sock.connected is True for cl in all_clients])
        assert len(set(all_clients)) == cl_count
    assert pool.done
    assert pool.available_clients is 0
