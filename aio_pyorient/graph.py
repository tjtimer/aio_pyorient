"""

 graph
"""
from aio_pyorient.pool import ODBPool
from aio_pyorient.utils import AsyncCtx


class ODBGraph(AsyncCtx):
    def __init__(self, user: str, password: str, db_name: str, **kwargs):
        super().__init__(**kwargs)
        self._pool = ODBPool(user, password, db_name=db_name, **kwargs)

    @property
    def pool_size(self):
        return self._pool.clients

    async def _setup(self):
        await self._pool.setup()

    async def _shutdown(self):
        await self._pool.shutdown()
