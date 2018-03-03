"""

 graph
"""
from aio_pyorient.pool import ODBPool
from aio_pyorient.utils import AsyncCtx


class ODBGraph(AsyncCtx):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self._vertex_registry = []
        self._edge_registry = []
        self._pool = ODBPool()
