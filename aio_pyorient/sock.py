import asyncio
import struct

from aio_pyorient.handler.base import short_packer
from aio_pyorient.utils import AsyncCtx


class ODBSocket(AsyncCtx):

    def __init__(self, *,
                 host: str="localhost", port: int=2424,
                 **kwargs):
        super().__init__(**kwargs)
        self._host = host
        self._port = port
        self._sent = asyncio.Event(loop=self._loop)
        self._reader, self._writer = None, None
        self._in_transaction = False
        self._props = None
        self.spawn(
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
        return self._is_ready.is_set()

    @property
    def in_transaction(self):
        return self._in_transaction

    async def connect(self):
        try:
            self._reader, self._writer = await asyncio.open_connection(
                self._host, self._port, loop=self._loop
            )
            self._sent.set()
            protocol = short_packer.unpack(await self._reader.readexactly(2))[0]
            self._is_ready.set()
            return protocol
        except asyncio.CancelledError:
            self._done.set()
            raise

    async def shutdown(self):
        self._cancelled.set()
        self._is_ready.clear()
        self._writer.close()
        self._reader.set_exception(asyncio.CancelledError())
        self._host = ""
        self._port = 0

    async def send(self, buff):
        await self._is_ready.wait()
        self._sent.clear()
        self._writer.write(buff)
        await self._writer.drain()
        self._sent.set()
        return len(buff)

    async def recv(self, _len_to_read):
        await self._sent.wait()
        buf = await self._reader.readexactly(_len_to_read)
        return buf
