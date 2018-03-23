from aio_pyorient.odb_types import ODBCluster

from aio_pyorient.handler.base import (
    BaseHandler
)
from aio_pyorient.handler.encoder import Boolean, String, Introduction, RequestHeader


class DbBaseHandler(BaseHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def read_clusters(self):
        for _ in range(await self.read_short()):
            yield ODBCluster(await self.read_string(), await self.read_short())

class OpenDb(DbBaseHandler):

    def __init__(
            self, client,
            db_name: str, user: str, password: str, *,
            client_id: str = '',
            use_token_auth: bool = True,
            support_push: bool = True,
            collect_stats: bool = True,
            **kwargs):
        super().__init__(
            client,
            (RequestHeader, (3, -1)),
            (Introduction, None),
            (String, client_id),
            (String, client._serialization_type),
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

    def __init__(self, client, **kwargs):
        super().__init__(
            client,
            (RequestHeader, (73, client._session_id, client._auth_token)),
            **kwargs
        )

    async def _read(self):
        self._client._clusters.clear()
        self._client._session_id = await self.read_int()
        self._client._auth_token = await self.read_bytes()
        async for cluster in self.read_clusters():
            self._client._clusters.append(cluster)
        return self._client

class CreateDb(BaseHandler):

    def __init__(self,
                 client,
                 db_name: str,
                 db_type: str="graph",
                 storage_type: str="plocal",
                 **kwargs):
        super().__init__(
            client,
            (RequestHeader, (4, client._session_id, client._auth_token)),
            (String, db_name),
            (String, db_type),
            (String, storage_type),
            **kwargs
        )

    async def _read(self):
        await self.read_header()
        return self._client

class DropDb(BaseHandler):

    def __init__(self,
                 client,
                 db_name: str,
                 storage_type: str="plocal",
                 **kwargs):
        super().__init__(
            client,
            (RequestHeader, (7, client._session_id, client._auth_token)),
            (String, db_name),
            (String, storage_type),
            **kwargs
        )

    async def _read(self):
        await self.read_header()
        return self._client

class DbExist(BaseHandler):

    def __init__(self,
                 client,
                 db_name: str,
                 storage_type: str,
                 **kwargs):
        super().__init__(
            client,
            (RequestHeader, (6, client._session_id, client._auth_token)),
            (String, db_name),
            (String, storage_type),
            **kwargs
        )

    async def _read(self):
        await self.read_header()
        return await self.read_bool()

class DbSize(BaseHandler):

    def __init__(self, client, **kwargs):
        super().__init__(
            client,
            (RequestHeader, (8, client._session_id, client._auth_token)),
            **kwargs
        )

    async def _read(self):
        await self.read_header()
        return await self.read_long()

class DbRecordCount(BaseHandler):

    def __init__(self, client, **kwargs):
        super().__init__(
            client,
            (RequestHeader, (9, client._session_id, client._auth_token)),
            **kwargs
        )

    async def _read(self):
        await self.read_header()
        return await self.read_long()

class CloseDb(BaseHandler):

    def __init__(self, client, **kwargs):
        super().__init__(
            client,
            (RequestHeader, (5, client._session_id, client._auth_token)),
            **kwargs
        )

    async def read(self):
        self._client._db_name = ''
        return self._client
