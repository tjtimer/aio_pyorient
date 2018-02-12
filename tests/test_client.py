import asyncio

from aio_pyorient import OrientDB


def test_connect():
    loop = asyncio.get_event_loop()
    client = OrientDB(loop=loop)
    session_id = loop.run_until_complete(
        client.connect(
            "root", "orient-pw"
        )
    )
    assert session_id >= 0
