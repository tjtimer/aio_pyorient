import asyncio
import socket
import struct

from aio_pyorient.constants import (
    FIELD_SHORT,
    SUPPORTED_PROTOCOL
)
from aio_pyorient.exceptions import (PyOrientConnectionException, PyOrientConnectionPoolException,
                                     PyOrientWrongProtocolVersionException)
from aio_pyorient.serializations import OrientSerialization


class OrientSocket(object):

    def __init__(self,
                 host, port, *,
                 serialization_type=OrientSerialization.CSV,
                 loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()):
        self._loop = loop
        self._connected = asyncio.Event(loop=self._loop)
        self.host = host
        self.port = port
        self._reader, self._writer = None, None
        self.protocol = -1
        self.session_id = -1
        self.auth_token = True
        self.db_opened = None
        self.serialization_type = serialization_type
        self.in_transaction = False
        self._props = None

    @property
    def connected(self):
        return self._connected.is_set()

    async def get_connection(self):
        if not self.connected:
            await self.connect()

        return self

    async def connect(self):
        try:
            self._reader, self._writer = await asyncio.open_connection(
                self.host, self.port, loop=self._loop
            )
            _value = await self._reader.read(FIELD_SHORT['bytes'])

            if len(_value) != 2:
                self._writer.close()

                raise PyOrientConnectionPoolException(
                    "Server sent empty string", []
                )

            self.protocol = struct.unpack('!h', _value)[0]
            if self.protocol > SUPPORTED_PROTOCOL:
                raise PyOrientWrongProtocolVersionException(
                    "Protocol version " + str(self.protocol) +
                    " is not supported yet by this client.", [])
            self._connected.set()
        except socket.error as e:
            self._connected.clear()
            raise PyOrientConnectionException("Socket Error: %s" % e, [])

    def close(self):
        '''Close the inner connection
        '''
        self.host = ''
        self.port = 0
        self.protocol = -1
        self.session_id = -1
        self._writer.close()
        self._connected.clear()

    async def write(self, buff):
        # This is a trick to detect server disconnection
        # or broken line issues because of
        """:see: https://docs.python.org/2/howto/sockets.html#when-sockets-die """
        """
        try:
            _, ready_to_write, in_error = select.select(
                [], [self._socket], [self._socket], 1)
        except select.error as e:
            self.connected = False
            self._socket.close()
            raise e

        if not in_error and ready_to_write:
            self._socket.sendall(buff)
            return len(buff)
        else:
            self.connected = False
            self._socket.close()
            raise PyOrientConnectionException("Socket error", [])
        """
        self._writer.write(buff)
        await self._writer.drain()
        return len(buff)

    # The man page for recv says: The receive calls normally return
    #   any data available, up to the requested amount, rather than waiting
    #   for receipt of the full amount requested.
    #
    # If you need to read a given number of bytes, you need to call recv
    #   in a loop and concatenate the returned packets until
    #   you have read enough.
    async def read(self, _len_to_read):
        buf = await self._reader.read(_len_to_read)
        return bytes(buf)
