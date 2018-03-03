"""

 graph
"""
from aio_pyorient.local_settings import PASSWORD, USER, DB_NAME
from aio_pyorient.pool import ODBPool
from aio_pyorient.utils import AsyncCtx


class ODBGraph(AsyncCtx):
    def __init__(self, *args, **kwargs):
        super().__init__(**kwargs)
        self._pool = ODBPool(USER, PASSWORD, db_name=DB_NAME, **kwargs)
