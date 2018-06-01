"""

 test_graph
"""
from pprint import pprint

from aio_pyorient.graph import ODBGraph
from aio_pyorient.odb_types import ODBSchema
from tests.conftest import TEST_USER, TEST_PASSWORD, TEST_DB


async def test_graph(loop):
    async with ODBGraph(TEST_USER, TEST_PASSWORD, TEST_DB, loop=loop) as graph:
        pprint('graph.schema')
        pprint(graph.schema)
        assert isinstance(graph.schema, ODBSchema)
        assert graph.classes == graph.schema.classes
        assert 'V' in graph.schema.classes.keys()
        assert 'E' in graph.schema.classes.keys()
