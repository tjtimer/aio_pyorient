import asyncio
import struct

from aio_pyorient.constants import NAME, SUPPORTED_PROTOCOL, VERSION
from aio_pyorient.handler import response_types
from aio_pyorient.handler.response_types import ErrorResponse
from aio_pyorient.otypes import ODBRecord, ODBCluster, ODBRequestErrorMessage
from aio_pyorient.utils import AsyncBase, ODBSignal

int_packer = struct.Struct("!i")
short_packer = struct.Struct("!h")
long_packer = struct.Struct("!q")

# response status
REQUEST_SUCCESS = 0
REQUEST_ERROR = 1
REQUEST_PUSH = 3

class Boolean:
    @staticmethod
    def encode(value):
        return bytes([1]) if value else bytes([0])

    @staticmethod
    async def decode(sock):
        decoded = ord(await sock.recv(1)) is 1
        return decoded

class Byte:
    @staticmethod
    def encode(value):
        return bytes([ord(value)])

    @staticmethod
    async def decode(sock):
        decoded = ord(await sock.recv(1))
        return decoded

class Bytes:
    @staticmethod
    def encode(value):
        return int_packer.pack(len(value)) + value

    @staticmethod
    async def decode(sock):
        value = b''
        _len = await Integer.decode(sock)
        if _len > 0:
            value = await sock.recv(_len)
        return value

class Char:
    @staticmethod
    def encode(value):
        return bytes(value, encoding="utf-8")

    @staticmethod
    async def decode(sock):
        value = await sock.recv(1)
        return value.decode()

class String:
    @staticmethod
    def encode(value):
        encoded = int_packer.pack(len(value)) + bytes(value, encoding="utf-8")
        return encoded

    @staticmethod
    async def decode(sock):
        decoded = ""
        _len = await Integer.decode(sock)
        if _len > 0:
            value = await sock.recv(_len)
            decoded = value.decode("utf-8")
        return decoded

class Integer:
    @staticmethod
    def encode(value):
        return int_packer.pack(value)

    @staticmethod
    async def decode(sock):
        decoded = int_packer.unpack(await sock.recv(4))[0]
        return decoded

class Short:
    @staticmethod
    def encode(value):
        return short_packer.pack(value)

    @staticmethod
    async def decode(sock):
        decoded = short_packer.unpack(await sock.recv(2))[0]
        return decoded

class Long:
    @staticmethod
    def encode(value):
        print("Long", value)
        return long_packer.pack(value)

    @staticmethod
    async def decode(sock):
        decoded = long_packer.unpack(await sock.recv(2))[0]
        return decoded

class RecordId:
    @staticmethod
    def encode(value):
        c_id, pos = value.replace("#", "").split(":")
        return Short.encode(int(c_id)) + Long.encode(pos)

    @staticmethod
    async def decode(sock):
        c_id, pos = [await Short.decode(sock), await Long.decode(sock)]
        return f"#{c_id}:{pos}"

class Record:
    @staticmethod
    def encode(value):
        fields = [
            Byte.encode(value.type.encode("utf-8")),
            RecordId.encode(value.id),
            Integer.encode(value.version),
            Bytes.encode(value.content)
        ]
        return b''.join(field for field in fields)

    @staticmethod
    async def decode(sock):
        r_type = await Char.decode(sock)
        r_id = await RecordId.decode(sock)
        r_version = await Integer.decode(sock)
        r_content = await Bytes.decode(sock)
        return ODBRecord(r_type, r_id, r_version, r_content)

class Link(RecordId):
    pass

class Introduction:
    @staticmethod
    def encode(_):
        return String.encode(NAME) + String.encode(VERSION) + Short.encode(SUPPORTED_PROTOCOL)

class RequestHeader:
    @staticmethod
    def encode(args):
        header = Byte.encode(chr(args[0])) + Integer.encode(args[1])
        if len(args) is 2:
            return header
        return header + Bytes.encode(args[2])

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
                    (RequestHeader, (Oeration: int, session_id, auth_token)),
                    (Field_1_Type, Field_1_Value),
                    ...
                )
                ...

            async def _read(self):
                ...
                return response
    """

    def __init__(self, client, *args,
                 ows_extra_payload=None,
                 ods_extra_payload=None,
                 owr_extra_payload=None,
                 odr_extra_payload=None,
                 no_ows=False,
                 no_ods=False,
                 no_owr=False,
                 no_odr=False):
        super().__init__(loop=client._loop)
        self._sent = asyncio.Event(loop=self._loop)
        self._client = client
        self._sock = client._sock
        self._serializer = client._serializer
        self._request = b''.join(
            field_type.encode(value)
            for field_type, value in args
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
        _len = await Integer.decode(self._sock)
        if _len > 0:
            value = await self._sock.recv(_len)
        return value

    async def read_char(self):
        value = await self._sock.recv(1)
        return value.decode()

    async def read_int(self):
        decoded = int_packer.unpack(await self._sock.recv(4))[0]
        return decoded

    async def read_long(self):
        decoded = long_packer.unpack(await self._sock.recv(2))[0]
        return decoded

    async def read_short(self):
        decoded = short_packer.unpack(await self._sock.recv(2))[0]
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
        finally:
            self._done.set()
            if not self._odr_disabled:
                await self.on_did_read.send(self)

    async def send(self):
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
            return (msg async for msg in self.read_error())
        if status is REQUEST_PUSH:
            return "PUSH MESSAGE"
        self._client._session_id = await self.read_int()
        if with_token:
            self._client._auth_token = await self.read_bytes()
        return self._client

    async def read_error(self):
        while True:
            more = await self.read_byte()
            if more is 0:
                break
            msg = ODBRequestErrorMessage(
                await self.read_string(),
                await self.read_string()
            )
            yield msg
        self._done.set()

    async def _read(self):
        """
        Overwrite this method if you want on_before_read
        and on_did_read signals to be send.
        """
        pass
