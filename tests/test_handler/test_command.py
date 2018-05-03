"""

 test_command
"""
import string

from hypothesis import strategies as st
from aio_pyorient.message.command import Query

async def test_select_command(binary_db_client):
    response = await binary_db_client.execute("select from Person")
    print("response:")
    for item in response:
        print(item)
        print(item.data.getvalue())

async def test_create_command(db_client):
    name = st.text(
        alphabet=[*string.ascii_uppercase, *string.ascii_lowercase],
        min_size=3, max_size=60).example()
    age = st.integers(min_value=10, max_value=150).example()
    print(name, age)
    handler = Query(
        db_client,
        f"CREATE VERTEX Person SET name='{name}', age={age}"
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
