"""

 graph
"""
from pprint import pprint

from aio_pyorient.message.command import Query
from aio_pyorient.odb_types import ODBSchema
from aio_pyorient.pool import ODBPool
from aio_pyorient.utils import AsyncCtx


SCHEMA_QUERY = "select expand(classes) from metadata:schema"


class ODBGraph(AsyncCtx):
    def __init__(self, user: str, password: str, db_name: str, **kwargs):
        super().__init__(**kwargs)
        self._pool = ODBPool(user, password, db_name=db_name, **kwargs)
        self._schema = None

    @property
    def pool_size(self):
        return self._pool.size

    @property
    def schema(self):
        return self._schema

    @property
    def classes(self):
        if self._schema is None:
            return None
        return self._schema.classes

    async def _setup(self):
        await self._pool.setup()
        self._schema = await self.get_schema()

    async def get_schema(self):
        handler = await Query(await self._pool.acquire(), SCHEMA_QUERY).send()
        result = await handler.read()
        return ODBSchema(result)
