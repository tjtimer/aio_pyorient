"""

 graph
"""

from aio_pyorient.pool import ODBPool
from aio_pyorient.utils import AsyncCtx

class ODBGraph(AsyncCtx):
    def __init__(self, user: str, password: str, db_name: str, **kwargs):
        super().__init__(**kwargs)
        self._pool = ODBPool(user, password, db_name=db_name, **kwargs)
        self._db_name = db_name
        self._schema = None

    @property
    def pool_size(self):
        return self._pool.clients

    @property
    def schema(self):
        if self._schema is None:
            return None
        return self._schema.classes

    async def _setup(self):
        await self._pool.setup()
        self._schema = await (await self._pool.acquire()).get_schema()


