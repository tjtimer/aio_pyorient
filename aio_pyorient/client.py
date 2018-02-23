import asyncio

from aio_pyorient.handler import db, server
from aio_pyorient.sock import ODBSocket

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass


class ODBClient:

    def __init__(self,
                 host: str = 'localhost',
                 port: int = 2424, *,
                 loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()):
        self._loop = loop
        self._sock = ODBSocket(
            host=host, port=port,
            loop=self._loop
        )

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

    async def connect(self, user: str, password: str, **kwargs):
        request = await server.Connect(self, user, password, **kwargs).send()
        return await request.read()

    async def open_db(self, db_name: str, user: str, password: str, **kwargs):
        request = await db.Open(self, db_name, user, password, **kwargs).send()
        return await request.read()

    async def reload_db(self, session_id: int, auth_token: bytes):
        request = await db.Reload(self, session_id, auth_token).send()
        return await request.read()
