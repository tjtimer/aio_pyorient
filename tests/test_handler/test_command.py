"""

 test_command
"""

from aio_pyorient.handler.command import QueryCommand


async def test_command(db_client):
    handler = QueryCommand(
        db_client,
        """UPDATE #22:0 MERGE {'age': 32} RETURN AFTER @this"""
    )
    await handler.send()
    response = await handler.read()
    print("response: ", list(response))
    assert handler.done
