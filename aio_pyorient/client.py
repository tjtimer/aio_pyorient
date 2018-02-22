import asyncio

from aio_pyorient.handler.base import (connect_response, db_open_response,
                                       db_reload_response, get_connect_request, get_db_open_request, get_request_header)
from aio_pyorient.sock import ODBSocket

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from aio_pyorient.serializations import OrientSerialization

type_map = {
    'BOOLEAN': 0,
    'INTEGER': 1,
    'SHORT': 2,
    'LONG': 3,
    'FLOAT': 4,
    'DOUBLE': 5,
    'DATETIME': 6,
    'STRING': 7,
    'BINARY': 8,
    'EMBEDDED': 9,
    'EMBEDDEDLIST': 10,
    'EMBEDDEDSET': 11,
    'EMBEDDEDMAP': 12,
    'LINK': 13,
    'LINKLIST': 14,
    'LINKSET': 15,
    'LINKMAP': 16,
    'BYTE': 17,
    'TRANSIENT': 18,
    'DATE': 19,
    'CUSTOM': 20,
    'DECIMAL': 21,
    'LINKBAG': 22,
    'ANY': 23
}


class ODBClient(object):
    _sock = None
    _session_id = -1
    _auth_token = b''
    _server_release = ''
    _cluster_conf = None

    def __init__(self,
                 host: str = 'localhost',
                 port: int = 2424, *,
                 serialization_type: OrientSerialization = OrientSerialization.CSV,
                 loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()):
        self._loop = loop
        self._sock = ODBSocket(
            host=host, port=port,
            loop=self._loop
        )
        self.serialization_type = serialization_type
        self.version = None
        self.clusters = []
        self.nodes = []

    @property
    def is_ready(self):
        return self._sock.connected

    @property
    def _cluster_map(self):
        return dict([(cluster.name, cluster.id) for cluster in self.clusters])

    @property
    def _cluster_reverse_map(self):
        return dict([(cluster.id, cluster.name) for cluster in self.clusters])

    def close(self):
        self._sock.close()
        if self._loop.is_running():
            self._loop.stop()
        if not self._loop.is_closed():
            self._loop.close()

    def get_class_position(self, cluster_name):
        return self._cluster_map[cluster_name.lower()]

    def get_class_name(self, position):
        return self._cluster_reverse_map[position]

    async def connect(self, user, password, **kwargs):
        await self._sock.write(get_connect_request(user, password, **kwargs))
        await connect_response(self)
        print(f"client connect_response:")
        print(f"\theader session_id: {self._session_id}")
        print(f"\theader auth_token: {self._auth_token}")
        return self._session_id, self._auth_token

    async def db_open(self, db_name, user, password, **kwargs):
        await self._sock.write(
            get_db_open_request(db_name, user, password, **kwargs)
        )
        await db_open_response(self)

        print(f"client db opened:")
        print(f"\tsession_id: {self._session_id}")
        print(f"\tauth_token: {self._auth_token}")
        print(f"\tsock vars: {vars(self._sock)}")
        return self.clusters

    async def db_reload(self):
        await self._send(get_request_header(73, self._session_id, self._auth_token))
        await db_reload_response(self)
        return self.clusters
