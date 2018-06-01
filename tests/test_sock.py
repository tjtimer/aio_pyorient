# test_sock
import asyncio
from pprint import pprint

from aio_pyorient.sock import ODBSocket


async def test_connect(loop):
    sock = ODBSocket()
    pprint(vars(sock))
    await asyncio.sleep(1)
    pprint(vars(sock))
    assert sock.connected is True
