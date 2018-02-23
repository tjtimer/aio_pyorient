from collections import namedtuple

from aio_pyorient.handler.base import BaseHandler, Boolean, Bytes, Integer, Introduction, RequestHeader, String
from aio_pyorient.serializations import OrientSerialization


class Open(BaseHandler):
    _result = namedtuple(
        "OpenDbResponse",
        "session_id, auth_token, cluster, cluster_conf, server_version"
    )

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

    async def read(self):
        await self.read_header(with_token=False)  # returns status, old session_id, empty byte
        s_id, token = [
            await Integer.decode(self._sock), await Bytes.decode(self._sock)
        ]
        cluster_list = []
        async for cluster in self.read_clusters():
            cluster_list.append(cluster)
        cluster_conf = await Bytes.decode(self._sock)
        server_version = await String.decode(self._sock)
        return self._result(s_id, token, cluster_list, cluster_conf, server_version)


class Reload(BaseHandler):
    _result = namedtuple(
        "ReloadDbResponse",
        "header, cluster"
    )

    def __init__(self, client, session_id: int, auth_token: bytes):
        super().__init__(
            client,
            (RequestHeader, (3, session_id, auth_token))
        )

    async def read(self):
        header = await self.read_header()
        cluster_list = []
        async for cluster in self.read_clusters():
            cluster_list.append(cluster)
        return self._result(header, cluster_list)
