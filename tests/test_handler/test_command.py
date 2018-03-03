"""

 test_command
"""

from aio_pyorient.handler.command import Query
from aio_pyorient.model.base import InitCommands


async def test_db_command(db_client):
    handler = Query(
        db_client,
        """select from person"""
    )
    await handler.send()
    response = await handler.read()
    print("response:")
    for item in response:
        print(item)
    assert handler.done
