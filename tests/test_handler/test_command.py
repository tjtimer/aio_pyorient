"""

 test_command
"""
from pprint import pprint

from aio_pyorient.handler.command import QueryCommand
from aio_pyorient.local_settings import TEST_DB_PASSWORD, TEST_USER, TEST_DB


async def test_command(client):
    await client.open_db(TEST_DB, TEST_USER, TEST_DB_PASSWORD)
    print("client before command")
    pprint(vars(client))
    handler = QueryCommand(
        client,
        """CREATE VERTEX Person CONTENT {"name": "tjtimer", "age": 32}"""
    )
    print("request: ", handler._request)
    await handler.send()
    response = await handler.read()
    print("response: ", response)
    pprint(vars(response[0]))
    assert handler.done
