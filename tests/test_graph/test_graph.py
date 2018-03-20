"""

 test_graph
"""
from pprint import pprint

from aio_pyorient.graph import ODBGraph
from aio_pyorient.odb_types import ODBSchema
from tests.test_settings import TEST_USER, TEST_PASSWORD, TEST_DB


async def test_graph():
    async with ODBGraph(TEST_USER, TEST_PASSWORD, TEST_DB) as graph:
        assert isinstance(graph._schema, ODBSchema)
        assert 'V' in graph._schema.classes.keys()
        assert 'E' in graph._schema.classes.keys()
        pprint(graph._schema.classes)
