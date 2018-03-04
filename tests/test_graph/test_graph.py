"""

 test_graph
"""
from pprint import pprint

from aio_pyorient.graph import ODBGraph
from tests.test_settings import TEST_USER, TEST_PASSWORD, TEST_DB


async def test_graph(loop):
    async with ODBGraph(TEST_USER, TEST_PASSWORD, TEST_DB, loop=loop) as graph:
        schema = await graph.get_schema()
        print(schema)
        assert schema is not None
