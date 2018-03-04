"""

 test_command
"""

from aio_pyorient.handler.command import Query

async def test_db_command(db_client):
    handler = Query(
        db_client,
        """select expand(classes) from metadata:schema"""
    )
    await handler.send()
    response = await handler.read()
    print("response:")
    for item in response:
        print(item)
    assert handler.done
