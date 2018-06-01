"""

 test_command
"""
import string

from hypothesis import strategies as st
from aio_pyorient.message.command import Query

async def test_select_command(db_client):
    response = await db_client.execute("select from #1:0")
    print("response:")
    for item in response:
        print(item)
        print(item.data.getvalue())

async def test_create_command(db_client):
    name = st.text(
        alphabet=[*string.ascii_uppercase, *string.ascii_lowercase],
        min_size=3, max_size=60).example()
    print(name)
    handler = Query(
        db_client,
        f"CREATE CLASS {name} EXTENDS VERTEX"
    )
    await handler.send()
    response = await handler.read()
    print("response:")
    for item in response:
        print(item)
    assert handler.done

async def test_update_command(db_client):
    age = st.integers(min_value=10, max_value=150).example()
    handler = Query(
        db_client,
        f"update #22:0 set age={age} return after @this"
    )
    await handler.send()
    response = await handler.read()
    print("response:")
    for item in response:
        print(item)
    assert handler.done
