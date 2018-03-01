import asyncio

from aio_pyorient.handler import db, server
from aio_pyorient.odb_types import ODBClusters
from aio_pyorient.serializations import OrientSerialization, OrientSerializationBinary, OrientSerializationCSV
from aio_pyorient.sock import ODBSocket
from aio_pyorient.utils import AsyncCtx


try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


class ODBClient(AsyncCtx):
    """
    ODBClient
    Use this to talk to your OrientDB server.

    """
    def __init__(self,
                 host: str = 'localhost',
                 port: int = 2424, **kwargs):
        super().__init__(**kwargs)
        self._sock = ODBSocket(host=host, port=port, loop=self._loop)
        self._id = kwargs.pop('client_id', '')
        self._session_id = kwargs.pop("session_id", -1)
        self._auth_token = kwargs.pop("auth_token", b'')
        self._db_name = kwargs.pop("db_name", '')
        self._clusters = kwargs.pop("clusters", ODBClusters())
        self._cluster_conf = kwargs.pop("cluster_conf", b'')
        self._server_version = kwargs.pop("server_version", '')
        self._protocol = None
        self._serialization_type = OrientSerialization.CSV
        self._is_ready = asyncio.Event(loop=self._loop)

    @property
    def is_ready(self):
        return self._sock.connected and self._is_ready.is_set()

    @property
    def protocol(self):
        return self._protocol

    @property
    def session_id(self):
        return self._session_id

    @property
    def auth_token(self):
        return self._auth_token

    @property
    def db_opened(self):
        return self._db_name

    @property
    def clusters(self):
        return self._clusters

    @property
    def cluster_conf(self):
        return self._cluster_conf

    @property
    def server_version(self):
        return self._server_version

    async def close(self):
        await self._sock.close()

    async def connect(self, user: str, password: str, **kwargs):
        request = await server.ServerConnect(self, user, password, **kwargs).send()
        return await request.read()

    async def open_db(self, db_name: str, user: str, password: str, **kwargs):
        request = await db.OpenDb(self, db_name, user, password, **kwargs).send()
        await request.read()
        return self

    async def reload_db(self, **kwargs):
        request = await db.ReloadDb(self, **kwargs).send()
        return await request.read()

    async def close_db(self, **kwargs):
        request = await db.CloseDb(self, **kwargs).send()
        return await request.read()
