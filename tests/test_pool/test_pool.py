"""

 test_pool
"""
import asyncio
import random
from pprint import pprint

from aio_pyorient.pool import ODBPool
from tests.conftest import TEST_USER, TEST_PASSWORD, TEST_DB

async def test_pool():
    cl_count = random.randint(50, 1500)
    print(f'test will acquire {cl_count} clients')
    async with ODBPool(TEST_USER, TEST_PASSWORD, db_name=TEST_DB) as pool:
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

async def test_pool_parallel(loop):
    async with ODBPool(TEST_USER, TEST_PASSWORD, db_name=TEST_DB, loop=loop) as pool:
        cl_1, cl_2, cl_3 , cl_4 = [await pool.acquire() for _ in range(4)]
        db_size, rec_count, schema, roles = await pool.wait_for(
            cl_1.db_size(),
            cl_2.db_record_count(),
            cl_3.execute('Select from #0:1'),
            cl_4.execute('Select from ORole')
        )
    print('db_size ', db_size)
    print('rec_count ', rec_count)
    print('\nSchema: ')
    pprint(vars(list(schema)[0]))
    print('\nroles: ')
    pprint(list(r.data.decode() for r in roles))
    assert db_size > 0
    assert rec_count > 0
    assert schema is not None
    assert roles is not None

async def test_client_ctx(loop):
    async with ODBPool(TEST_USER, TEST_PASSWORD, db_name=TEST_DB, loop=loop) as pool:
        client = await pool.acquire()
        res = await client.execute('Select From OUser')
    print('results: ')
    pprint(list(res))
    assert res is not None
