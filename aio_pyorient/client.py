import asyncio

from aio_pyorient.handler import db, server, command
from aio_pyorient.odb_types import ODBClusters, ODBSchema
from aio_pyorient.sock import ODBSocket
from aio_pyorient.utils import AsyncCtx


try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


SCHEMA_QUERY = "select expand(classes) from metadata:schema"


class ODBClient(AsyncCtx):
    """
    ODBClient
    Use this to talk to your OrientDB server.

    """
    _serialization_type = "ORecordDocument2csv"  #
    # _serialization_type = "ORecordSerializerBinary"
    def __init__(self,
                 host: str = 'localhost',
                 port: int = 2424,
                 auth_token: bytes = b'',
                 client_id: str = '',
                 clusters: ODBClusters = ODBClusters(),
                 cluster_conf: bytes = b'',
                 db_name: str = '',
                 server_version: str = '',
                 session_id: int = -1, **kwargs):
        super().__init__(**kwargs)
        self._sock = ODBSocket(host=host, port=port, loop=self._loop)
        self._auth_token = auth_token
        self._id = client_id
        self._clusters = clusters
        self._cluster_conf = cluster_conf
        self._db_name = db_name
        self._server_version = server_version
        self._session_id = session_id
        self._protocol = None
        self._watcher = {}
        self._is_ready.set()

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
    def active_db(self):
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

    @property
    def watcher(self):
        return self._watcher

    async def _shutdown(self):
        await self._sock.shutdown()

    async def connect(self, user: str, password: str, **kwargs):
        handler = await server.ServerConnect(self, user, password, **kwargs).send()
        return await handler.read()

    async def create_db(self, db_name: str, storage_type: str, **kwargs):
        handler = await db.CreateDb(self, db_name, storage_type, **kwargs).send()
        return await handler.read()

    async def open_db(self, db_name: str, user: str, password: str, **kwargs):
        handler = await db.OpenDb(self, db_name, user, password, **kwargs).send()
        return await handler.read()

    async def reload_db(self, **kwargs):
        handler = await db.ReloadDb(self, **kwargs).send()
        return await handler.read()

    async def close_db(self, **kwargs):
        handler = await db.CloseDb(self, **kwargs).send()
        return await handler.read()

    async def db_exist(self, db_name: str, storage_type: str, **kwargs):
        handler = await db.DbExist(self, db_name, storage_type, **kwargs).send()
        return await handler.read()

    async def db_size(self, **kwargs):
        handler = await db.DbSize(self, **kwargs).send()
        return await handler.read()

    async def db_record_count(self, **kwargs):
        handler = await db.DbRecordCount(self, **kwargs).send()
        return await handler.read()

    async def get_schema(self):
        handler = await command.Query(self, SCHEMA_QUERY).send()
        result = await handler.read()
        return ODBSchema(result)

    async def watch_schema(self):
        handler = await command.Query(self, "LIVE SELECT FROM '0:1").send()
        result = await handler.read()
        return ODBSchema(result)

    async def execute(self, query: str, **kwargs):
        handler = await command.Query(self, query, **kwargs).send()
        return await handler.read()
