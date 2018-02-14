import asyncio
import struct

from aio_pyorient.constants import (
    FIELD_SHORT,
    SUPPORTED_PROTOCOL
)
from aio_pyorient.exceptions import (PyOrientConnectionPoolException,
                                     PyOrientWrongProtocolVersionException)
from aio_pyorient.serializations import OrientSerialization


class OrientSocket(object):

    def __init__(self,*,
                 host: str="localhost", port: int=2424,
                 serialization_type=OrientSerialization.CSV,
                 loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()):
        self._loop = loop
        self._connected = asyncio.Event(loop=self._loop)
        self._host = host
        self._port = port
        self._reader, self._writer = None, None
        self.protocol = None
        self.session_id = -1
        self.auth_token = True
        self.db_opened = None
        self.serialization_type = serialization_type
        self.in_transaction = False
        self._props = None

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def connected(self):
        return self._connected.is_set()

    async def get_connection(self):
        if not self.connected:
            await self.connect()

        return self

    async def connect(self):
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port, loop=self._loop
        )
        connect_response = await self._reader.read(FIELD_SHORT['bytes'])

        if len(connect_response) != 2:
            self._writer.close()
            raise PyOrientConnectionPoolException(
                "Server sent empty string", []
            )

        self.protocol = struct.unpack('>h', connect_response)[0]
        if self.protocol > SUPPORTED_PROTOCOL:
            raise PyOrientWrongProtocolVersionException(
                "Protocol version " + str(self.protocol) +
                " is not supported yet by this client.", [])
        self._connected.set()

    def close(self):
        self._connected.clear()
        self._writer.close()
        self._host = ''
        self._port = 0
        self.protocol = None
        self.session_id = -1

    async def write(self, buff):
        self._writer.write(buff)
        await self._writer.drain()
        return len(buff)

    async def read(self, _len_to_read):
        buf = await self._reader.read(_len_to_read)
        self._reader.feed_data(buf)
        return bytes(buf)
