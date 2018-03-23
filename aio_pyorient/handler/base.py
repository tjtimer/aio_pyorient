import asyncio
import struct

from aio_pyorient.odb_types import (
    ODBRequestErrorMessage
)
from aio_pyorient.utils import AsyncBase, ODBSignal


NAME = "ODB binary client (aio_pyorient)"
VERSION = "0.0.1"
SUPPORTED_PROTOCOL = 37

# response status
REQUEST_SUCCESS = 0
REQUEST_ERROR = 1
REQUEST_PUSH = 3


class ODBHandlerError(BaseException):
    pass


class BaseHandler(AsyncBase):
    """
    # BaseHandler
    Parent class for all client server communication handlers.

    Child classes should overwrite _read method.

    It provides common properties and methods
    like reading and decoding different fields,
    reading response header (status, session_id, auth_token), etc.

    Usage:
        class MyHandler(BaseHandler):
            def __init__(self, client, *args, **kwargs):
                super().__init__(
                    client,
                    (RequestHeader, (
                            Operation: int,
                            client._session_id,
                            client._auth_token)),
                    (Type_Field_1, Value_Field_1),
                    (Type_Field_2, Value_Field_2),
                    ...
                    **kwargs
                )
                ...

            async def _read(self):
                # read field after field from socket
                return response
    """

    def __init__(self, client, *fields,
                 ows_extra_payload=None,
                 ods_extra_payload=None,
                 owr_extra_payload=None,
                 odr_extra_payload=None,
                 no_ows=False,
                 no_ods=False,
                 no_owr=False,
                 no_odr=False,
                 **kwargs):
        super().__init__(loop=client._loop)
        self._sent = asyncio.Event(loop=self._loop)
        self._client = client
        self._sock = client._sock
        self._request = b''.join(
            field_type(value)
            for field_type, value in fields
        )
        self.on_will_send = ODBSignal(self, ows_extra_payload)
        self.on_will_read = ODBSignal(self, ods_extra_payload)
        self.on_did_send = ODBSignal(self, owr_extra_payload)
        self.on_did_read = ODBSignal(self, odr_extra_payload)
        self._ows_disabled = no_ows
        self._ods_disabled = no_ods
        self._owr_disabled = no_owr
        self._odr_disabled = no_odr

    async def read_bool(self):
        return ord(await self._sock.recv(1)) is 1

    async def read_byte(self):
        return ord(await self._sock.recv(1))

    async def read_bytes(self):
        value = b''
        _len = await self.read_int()
        if _len > 0:
            value = await self._sock.recv(_len)
        return value

    async def read_char(self):
        value = await self._sock.recv(1)
        return value.decode()

    async def read_short(self):
        decoded = short_packer.unpack(await self._sock.recv(2))[0]
        return decoded

    async def read_int(self):
        decoded = int_packer.unpack(await self._sock.recv(4))[0]
        return decoded

    async def read_long(self):
        decoded = long_packer.unpack(await self._sock.recv(8))[0]
        return decoded

    async def read_string(self):
        decoded = ""
        _len = await self.read_int()
        if _len > 0:
            value = await self._sock.recv(_len)
            decoded = value.decode("utf-8")
        return decoded

    async def read(self):
        if not self._owr_disabled:
            await self.on_will_read.send(self)
        try:
            return await self._read()
        except ODBHandlerError:
            return await  self.read_error()
        finally:
            self._done.set()
            self._client._is_ready.set()
            if not self._odr_disabled:
                await self.on_did_read.send(self)

    async def send(self):
        self._client._is_ready.clear()
        if not self._ows_disabled:
            await self.on_will_send.send(self)
        try:
            await self._sock.send(self._request)
            self._sent.set()
            return self
        finally:
            if not self._ods_disabled:
                await self.on_did_send.send(self)

    async def read_header(self, with_token: bool = True):
        await self._sent.wait()
        status = await self.read_byte()
        if status is REQUEST_ERROR:
            raise ODBHandlerError()
        if status is REQUEST_PUSH:
            print("receiving push message")
            return "PUSH MESSAGE"
        self._client._session_id = await self.read_int()
        if with_token:
            self._client._auth_token = await self.read_bytes()
        return self._client

    async def read_error(self):
        messages = []
        while True:
            more = await self.read_byte()
            print("error more: ", more)
            if more is 0:
                break
            messages.append(ODBRequestErrorMessage(
                await self.read_string(),
                await self.read_string()
            ))
        self._done.set()
        return messages

    async def _read(self):
        """
        Overwrite this method if you want on_before_read
        and on_did_read signals to be send.
        """
        pass


int_packer = struct.Struct("!i")
short_packer = struct.Struct("!h")
long_packer = struct.Struct("!q")
