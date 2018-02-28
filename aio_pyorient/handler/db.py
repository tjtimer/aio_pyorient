from aio_pyorient.handler.base import (
    BaseHandler, Boolean, Bytes, Integer, Introduction, RequestHeader, String, Long
)
from aio_pyorient.otypes import ODBCluster
from aio_pyorient.handler.response_types import OpenDbResponse, ReloadDbResponse
from aio_pyorient.serializations import OrientSerialization

class DbBaseHandler(BaseHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def read_clusters(self):
        for _ in range(await self.read_short()):
            yield ODBCluster(await self.read_string(), await self.read_short())

class OpenDb(DbBaseHandler):

    def __init__(
            self, client, db_name: str, user: str, password: str, *,
            client_id: str = '',
            serialization_type: OrientSerialization = OrientSerialization.CSV,
            use_token_auth: bool = True,
            support_push: bool = True,
            collect_stats: bool = True,
            **kwargs):
        super().__init__(
            client,
            (RequestHeader, (3, -1)),
            (Introduction, None),
            (String, client_id),
            (String, serialization_type),
            (Boolean, use_token_auth),
            (Boolean, support_push),
            (Boolean, collect_stats),
            (String, db_name),
            (String, user),
            (String, password),
            **kwargs
        )
        self._db_name = db_name

    async def _read(self):
        await self.read_header(with_token=False)  # returns status, old session_id, empty byte
        self._client._session_id = await self.read_int()
        self._client._auth_token = await self.read_bytes()
        async for cluster in self.read_clusters():
            self._client._clusters.append(cluster)
        self._client._cluster_conf = await self.read_bytes()
        self._client._server_version = await self.read_string()
        self._client._db_name = self._db_name
        return self._client

class ReloadDb(DbBaseHandler):

    def __init__(self, client, session_id: int, auth_token: bytes):
        super().__init__(
            client,
            (RequestHeader, (73, session_id, auth_token))
        )

    async def _read(self):
        self._client._session_id = await self.read_int()
        self._client._auth_token = await self.read_bytes()
        async for cluster in self.read_clusters():
            self._client._clusters.append(cluster)
        return self._client

class CreateDb(BaseHandler):

    def __init__(self,
                 client,
                 session_id: int,
                 auth_token: bytes,
                 db_name: str,
                 db_type: str="graph",
                 storage_type: str="plocal"):
        super().__init__(
            client,
            (RequestHeader, (4, session_id, auth_token)),
            (String, db_name),
            (String, db_type),
            (String, storage_type)
        )

    async def _read(self):
        await self.read_header()
        return self._client

class DropDb(BaseHandler):

    def __init__(self,
                 client,
                 session_id: int,
                 auth_token: bytes,
                 db_name: str,
                 storage_type: str="plocal"):
        super().__init__(
            client,
            (RequestHeader, (7, session_id, auth_token)),
            (String, db_name),
            (String, storage_type)
        )

    async def _read(self):
        await self.read_header()
        return self._client

class DbExist(BaseHandler):

    def __init__(self,
                 client,
                 session_id: int,
                 auth_token: bytes,
                 db_name: str,
                 storage_type: str):
        super().__init__(
            client,
            (RequestHeader, (6, session_id, auth_token)),
            (String, db_name),
            (String, storage_type)
        )

    async def _read(self):
        await self.read_header()
        return await self.read_bool()

class DbSize(BaseHandler):

    def __init__(self, client, session_id: int, auth_token: bytes):
        super().__init__(
            client,
            (RequestHeader, (8, session_id, auth_token))
        )

    async def _read(self):
        await self.read_header()
        return await self.read_long()



class DbRecordCount(BaseHandler):

    def __init__(self, client, session_id: int, auth_token: bytes):
        super().__init__(
            client,
            (RequestHeader, (9, session_id, auth_token))
        )

    async def _read(self):
        await self.read_header()
        return await self.read_long()

class CloseDb(BaseHandler):

    def __init__(self, client, session_id: int, auth_token: bytes):
        super().__init__(
            client,
            (RequestHeader, (5, session_id, auth_token))
        )

    async def read(self):
        return "Database closed!"
