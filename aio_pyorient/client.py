import asyncio
from pprint import pprint

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:
    pass

from aio_pyorient.messages.cluster import (
    DataClusterAddMessage,
    DataClusterCountMessage,
    DataClusterDataRangeMessage,
    DataClusterDropMessage
)
from aio_pyorient.messages.commands import CommandMessage, TxCommitMessage
from aio_pyorient.messages.connection import ConnectMessage, ShutdownMessage
from aio_pyorient.messages.database import (
    DbCloseMessage,
    DbCountRecordsMessage,
    DbCreateMessage,
    DbDropMessage,
    DbExistsMessage,
    DbListMessage,
    DbOpenMessage,
    DbReloadMessage,
    DbSizeMessage
)
from aio_pyorient.messages.records import (
    RecordCreateMessage,
    RecordDeleteMessage,
    RecordLoadMessage,
    RecordUpdateMessage
)
from aio_pyorient.sock import OrientSocket
from aio_pyorient.constants import (
    QUERY_ASYNC,
    QUERY_CMD,
    QUERY_GREMLIN,
    QUERY_SCRIPT,
    QUERY_SYNC,
    STORAGE_TYPE_PLOCAL,
    DB_TYPE_GRAPH)
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


class OrientDB(object):
    """OrientDB client object

    Point of entrance to use the basic commands you can issue to the server

    :param host: hostname of the server to connect  defaults to localhost
    :param port: integer port of the server         defaults to 2424

    Usage::

        >>> from aio_pyorient import OrientDB
        >>> client = OrientDB("localhost", 2424)
        >>> client.db_open('MyDatabase', 'admin', 'admin')


    """
    _connection = None
    _auth_token = None

    def __init__(self,
                 host: str = 'localhost',
                 port: int = 2424, *,
                 serialization_type: OrientSerialization = OrientSerialization.CSV,
                 loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()):
        self._loop = loop
        self._connection = OrientSocket(
            host=host, port=port,
            serialization_type=serialization_type, loop=self._loop
        )
        self._serialization_type = serialization_type
        self.version = None
        self.clusters = []
        self.nodes = []

    @property
    def serialization_type(self):
        return self._connection.serialization_type

    @property
    def _cluster_map(self):
        return dict([(cluster.name, cluster.id) for cluster in self.clusters])

    @property
    def _cluster_reverse_map(self):
        return dict([(cluster.id, cluster.name) for cluster in self.clusters])

    def close(self):
        self._connection.close()

    def get_class_position(self, cluster_name):
        return self._cluster_map[cluster_name.lower()]

    def get_class_name(self, position):
        return self._cluster_reverse_map[position]

    def set_session_token(self, token):
        """
        Set true if you want to use token authentication
        :param token: bool
        """
        self._auth_token = token
        return self

    def get_session_token(self):
        """Returns the auth token of the session
        """
        return self._connection.auth_token

    async def connect(self, user, password, client_id=''):
        await self._connection.get_connection()
        request = ConnectMessage(self._connection).prepare(
            (user, password, client_id, self.serialization_type)
        )
        await request.send()
        response = await request.fetch_response()
        print("client connect response")
        print(response)
        print("client vars")
        pprint(vars(self))
        print("client connection vars")
        pprint(vars(self._connection))
        return response

    async def db_count_records(self):
        request = DbCountRecordsMessage(self._connection).prepare(())
        await request.send()
        result = await request.fetch_response()
        return result

    async def db_create(self, name, type=DB_TYPE_GRAPH, storage=STORAGE_TYPE_PLOCAL):
        request = DbCreateMessage(self._connection).prepare(
            (name, type, storage))
        await request.send()
        await request.fetch_response()
        return None

    async def db_drop(self, name, type=STORAGE_TYPE_PLOCAL):
        request = DbDropMessage(self._connection).prepare((name, type))
        await request.send()
        await request.fetch_response()
        return None

    async def db_exists(self, name, type=STORAGE_TYPE_PLOCAL):
        request = DbExistsMessage(self._connection).prepare((name, type))
        await request.send()
        result = await request.fetch_response()
        return result

    async def db_open(self,
                      db_name, user, password,
                      db_type=DB_TYPE_GRAPH, client_id=''):
        await self._connection.get_connection()
        request = DbOpenMessage(self._connection).prepare(
            (db_name, user, password, db_type, client_id))
        await request.send()
        response = await request.fetch_response()
        self.version, self.clusters, self.nodes = response
        await self.update_properties()
        return self.clusters

    async def db_reload(self):
        request = DbReloadMessage(self._connection).prepare(())
        await request.send()
        self.clusters = await request.fetch_response()
        await self.update_properties()
        return self.clusters

    async def update_properties(self):
        if self.serialization_type == OrientSerialization.Binary:
            self._connection._props = {
                x['id']: [x['name'], type_map[x['type']]]
                for x in await self.command(
                "select from #0:1"
            )[0].oRecordData['globalProperties']
            }

    async def shutdown(self, *args):
        request = ShutdownMessage(self._connection).prepare(args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def gremlin(self, *args):
        request = CommandMessage(self._connection).prepare(
            (QUERY_GREMLIN,) + args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def command(self, *args):
        request = CommandMessage(self._connection).prepare(
            (QUERY_CMD,) + args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def batch(self, *args):
        request = CommandMessage(self._connection).prepare(
            (QUERY_SCRIPT,) + args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def query(self, *args):
        request = CommandMessage(self._connection).prepare(
            (QUERY_SYNC,) + args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def query_async(self, *args):
        request = CommandMessage(self._connection).prepare(
            (QUERY_ASYNC,) + args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def data_cluster_add(self, *args):
        request = DataClusterAddMessage(self._connection).prepare(
            args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def data_cluster_count(self, *args):
        request = DataClusterCountMessage(self._connection).prepare(
            args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def data_cluster_data_range(self, *args):
        request = DataClusterDataRangeMessage(self._connection).prepare(
            args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def data_cluster_drop(self, *args):
        request = DataClusterDropMessage(self._connection).prepare(
            args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def db_close(self, *args):
        request = DbCloseMessage(self._connection).prepare(
            args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def db_size(self, *args):
        request = DbSizeMessage(self._connection).prepare(
            args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def db_list(self, *args):
        request = DbListMessage(self._connection).prepare(
            args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def record_create(self, *args):
        request = RecordCreateMessage(self._connection).prepare(
            args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def record_delete(self, *args):
        request = RecordDeleteMessage().prepare(args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def record_load(self, *args):
        request = RecordLoadMessage(self._connection).prepare(
            args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def record_update(self, *args):
        request = RecordUpdateMessage(self._connection).prepare(
            args)
        await request.send()
        response = await request.fetch_response()
        return response

    async def tx_commit(self):
        return TxCommitMessage(self._connection)

    def _push_received(self, command_id, *args):
        # REQUEST_PUSH_RECORD	        79
        # REQUEST_PUSH_DISTRIB_CONFIG	80
        # REQUEST_PUSH_LIVE_QUERY	    81
        # TODO: this logic must stay within Messages class here I just want to receive
        # an object of something, like a new array of cluster.
        if command_id == 80:
            pass
