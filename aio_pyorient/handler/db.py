from aio_pyorient.handler.base import BaseHandler, Boolean, Bytes, Integer, Introduction, RequestHeader, String, Long
from aio_pyorient.handler.response_types import OpenDbResponse, ReloadDbResponse
from aio_pyorient.serializations import OrientSerialization



class OpenDb(BaseHandler):

    def __init__(
            self, client, db_name: str, user: str, password: str, *,
            client_id: str = '',
            serialization_type: OrientSerialization = OrientSerialization.CSV,
            use_token_auth: bool = True,
            support_push: bool = True,
            collect_stats: bool = True):
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
            (String, password)
        )

    async def _read(self):
        await self.read_header(with_token=False)  # returns status, old session_id, empty byte
        s_id, token = [
            await Integer.decode(self._sock), await Bytes.decode(self._sock)
        ]
        cluster_list = []
        async for cluster in self.read_clusters():
            cluster_list.append(cluster)
        cluster_conf = await Bytes.decode(self._sock)
        server_version = await String.decode(self._sock)
        return self.response_type(s_id, token, cluster_list, cluster_conf, server_version)



class ReloadDb(BaseHandler):

    def __init__(self, client, session_id: int, auth_token: bytes):
        super().__init__(
            client,
            (RequestHeader, (73, session_id, auth_token))
        )

    async def _read(self):
        header = await self.read_header()
        cluster_list = []
        async for cluster in self.read_clusters():
            cluster_list.append(cluster)
        return self.response_type(header, cluster_list)



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
        s_id, auth_token = await self.read_header()
        return self.response_type(s_id, auth_token)



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
        s_id, auth_token = await self.read_header()
        return self.response_type(s_id, auth_token)



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
        s_id, auth_token = await self.read_header()
        exist = await Boolean.decode(self._sock)
        return self.response_type(s_id, auth_token, exist)



class DbSize(BaseHandler):

    def __init__(self, client, session_id: int, auth_token: bytes):
        super().__init__(
            client,
            (RequestHeader, (8, session_id, auth_token))
        )

    async def _read(self):
        s_id, auth_token = await self.read_header()
        size = await Long.decode(self._sock)
        return self.response_type(s_id, auth_token, size)



class DbRecordCount(BaseHandler):

    def __init__(self, client, session_id: int, auth_token: bytes):
        super().__init__(
            client,
            (RequestHeader, (9, session_id, auth_token))
        )

    async def _read(self):
        s_id, auth_token = await self.read_header()
        count = await Long.decode(self._sock)
        return self.response_type(s_id, auth_token, count)



class CloseDb(BaseHandler):

    def __init__(self, client, session_id: int, auth_token: bytes):
        super().__init__(
            client,
            (RequestHeader, (5, session_id, auth_token))
        )

    async def read(self):
        return "Database closed!"
