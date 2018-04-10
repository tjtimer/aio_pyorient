import asyncio

from aio_pyorient.handler import db, server, command
from aio_pyorient.handler.base import int_packer
from aio_pyorient.model.prop_types import TYPE_MAP
from aio_pyorient.odb_types import ODBClusters
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
                 client_id: str='',
                 session_id: int=-1,
                 auth_token: bytes = b'',
                 db_name: str=None,
                 clusters: ODBClusters=ODBClusters(),
                 cluster_conf: bytes=b'',
                 server_version: str= '',
                 protocol: int=None,
                 serialization_type="ORecordDocument2csv",
                 host: str = 'localhost',
                 port: int = 2424, **kwargs):
        super().__init__(**kwargs)
        self._sock = ODBSocket(host=host, port=port, loop=self._loop)
        self._id = client_id
        self._session_id = session_id
        self._auth_token = auth_token
        self._db_name = db_name
        self._clusters = clusters
        self._cluster_conf = cluster_conf
        self._server_version = server_version
        self._protocol = protocol

        # "ORecordSerializerBinary" or "ORecordDocument2csv"
        self._serialization_type = serialization_type
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

    async def execute(self, query: str, **kwargs):
        handler = await command.Query(self, query, **kwargs).send()
        return await handler.read()

    async def get_global_props(self):
        response = await self.execute("select globalProperties from #0:1")
        result = []
        rest = None
        for record in response:
            raw = record.data
            print(raw)
            raw.read(2)
            i = 0
            cur = 2
            while True:
                next_b = raw.read(1)
                cur += 1
                if next_b == b'':
                    break
                num_val = ord(next_b)  >> 1
                if num_val is 0:
                    rest = raw.getvalue()[cur:]
                    break
                name = raw.read(num_val)
                cur += num_val
                position = int_packer.unpack(raw.read(4))[0]
                data_type = TYPE_MAP[ord(raw.read(1))]
                data_def = (name, position, data_type)
                cur += 5
                print(f'{i}: {data_def}')
                i += 1
                result.append(data_def)
        print('result')
        print(result)
        print('rest')
        print(rest)
        return result, rest
