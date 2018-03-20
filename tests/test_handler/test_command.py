"""

 test_command
"""
from aio_pyorient.client import SCHEMA_QUERY
from aio_pyorient.handler.command import Query

async def test_db_command(db_client):
    handler = Query(
        db_client,
        f"""select from ({SCHEMA_QUERY}) where superClasses in [\"V\", \"E\"]"""
    )
    await handler.send()
    response = await handler.read()
    print("response:")
    for item in response:
        print(item)
    assert handler.done
