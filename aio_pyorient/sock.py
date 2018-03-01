import asyncio
import struct

from aio_pyorient.constants import SUPPORTED_PROTOCOL
from aio_pyorient.exceptions import (PyOrientWrongProtocolVersionException)
from aio_pyorient.utils import AsyncCtx


class ODBSocket(AsyncCtx):

    def __init__(self, *,
                 host: str="localhost", port: int=2424,
                 **kwargs):
        super().__init__(**kwargs)
        self._host = host
        self._port = port
        self._connected = asyncio.Event(loop=self._loop)
        self._sent = asyncio.Event(loop=self._loop)
        self._reader, self._writer = None, None
        self._in_transaction = False
        self._props = None
        self.create_task(
            self.connect
        )

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def connected(self):
        return self._connected.is_set()

    @property
    def in_transaction(self):
        return self._in_transaction

    async def connect(self):
        self._reader, self._writer = await asyncio.open_connection(
            self._host, self._port, loop=self._loop
        )
        protocol = struct.unpack(">h", await self._reader.readexactly(2))[0]
        if protocol > SUPPORTED_PROTOCOL:
            raise PyOrientWrongProtocolVersionException(
                "Protocol version " + str(protocol) +
                " is not supported yet by this client.", [])
        self._connected.set()
        return protocol

    async def _close(self):
        self._connected.clear()
        self._writer.close()
        self._host = ""
        self._port = 0

    async def send(self, buff):
        await self._connected.wait()
        self._sent.clear()
        self._writer.write(buff)
        await self._writer.drain()
        self._sent.set()
        return len(buff)

    async def recv(self, _len_to_read):
        await self._sent.wait()
        buf = await self._reader.readexactly(_len_to_read)
        return buf
