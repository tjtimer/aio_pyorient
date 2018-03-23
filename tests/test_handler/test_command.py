"""

 test_command
"""
import random
import string
import time

from hypothesis import given, strategies as st, settings

from aio_pyorient.handler.base import int_packer
from aio_pyorient.handler.command import Query
from aio_pyorient.odb_types import ODBRecord


async def test_select_command(db_client):
    handler = Query(
        db_client,
        f"""select from #0:1"""
    )
    await handler.send()
    response = await handler.read()
    print("response:")
    for item in response:
        print(item)
    assert handler.done
"""
@given(
    name=st.text(alphabet=string.ascii_letters, min_size=3, max_size=20),
    age=st.integers(min_value=10, max_value=150))
"""
# def test_create_(name, age, db_client):
async def test_create_command(db_client):
    name = st.text(alphabet=[*string.ascii_lowercase, *string.ascii_uppercase], min_size=3, max_size=25).example()
    age = st.integers(min_value=2, max_value=120).example()
    print(name, age)
    handler = Query(
        db_client,
        f"""CREATE VERTEX Person SET name="{name}", age={age}, email='{name}@mail.ex RETURN AFTER @this"""
    )
    await handler.send()
    response = await handler.read()
    print("response:")
    print(response)
    for item in response:
        print(item)
    assert handler.done
    # db_client.spawn(test_create_command(name, age))
    # time.sleep(1)

async def test_update_command(db_client):
    age = random.randint(15, 100)
    handler = Query(
        db_client,
        f"""UPDATE #22:0 set age={age} RETURN AFTER @this"""
    )
    await handler.send()
    response = await handler.read()
    print("response:")
    for item in response:
        assert isinstance(item, ODBRecord)
        assert item.id is not None
    assert handler.done
