import asyncio
import inspect
import struct
from collections import namedtuple

from aio_pyorient.constants import NAME, SUPPORTED_PROTOCOL, VERSION
from aio_pyorient.handler.fields import Byte, Bytes, Integer, Short, String
from aio_pyorient.utils import AsyncObject

int_packer = struct.Struct("!i")
short_packer = struct.Struct("!h")
long_packer = struct.Struct("!q")

ODBRecord = namedtuple("ODBRecord", "type, id, version, content")
ODBResponseHeader = namedtuple('ODBResponseHeader', 'status, session_id, auth_token')
ODBCluster = namedtuple('ODBCluster', 'name, id')

class Boolean:

    @staticmethod
    def encode(value):
        print("Boolean", value)
        return bytes([1]) if value else bytes([0])

    @staticmethod
    async def decode(sock):
        decoded = ord(await sock.recv(1)) is 1
        print(decoded)
        return decoded


class Byte:

    @staticmethod
    def encode(value):
        print("Byte", value)
        return bytes([ord(value)])

    @staticmethod
    async def decode(sock):
        decoded = ord(await sock.recv(1))
        print(decoded)
        return decoded


class Bytes:

    @staticmethod
    def encode(value):
        print("Bytes", value)
        return int_packer.pack(len(value)) + value

    @staticmethod
    async def decode(sock):
        value = b''
        _len = await Integer.decode(sock)
        if _len > 0:
            value = await sock.recv(_len)
        print(value)
        return value


class String:

    @staticmethod
    def encode(value):
        print(value, type(value))
        encoded = int_packer.pack(len(value)) + bytes(value, encoding="utf-8")
        print(encoded)
        return encoded

    @staticmethod
    async def decode(sock):
        decoded = ""
        _len = await Integer.decode(sock)
        if _len > 0:
            value = await sock.recv(_len)
            decoded = value.decode("utf-8")
        print(decoded)
        return decoded


class Integer:

    @staticmethod
    def encode(value):
        print("Integer", value)
        return int_packer.pack(value)

    @staticmethod
    async def decode(sock):
        decoded = int_packer.unpack(await sock.recv(4))[0]
        print(decoded)
        return decoded


class Short:

    @staticmethod
    def encode(value):
        print("Short", value)
        return short_packer.pack(value)

    @staticmethod
    async def decode(sock):
        decoded = short_packer.unpack(await sock.recv(2))[0]
        print(decoded)
        return decoded


class Long:

    @staticmethod
    def encode(value):
        print("Long", value)
        return long_packer.pack(value)

    @staticmethod
    async def decode(sock):
        decoded = long_packer.unpack(await sock.recv(2))[0]
        print(decoded)
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
        r_type = str(await Byte.decode(sock))
        r_id = await RecordId.decode(sock)
        r_version = await Integer.decode(sock)
        r_content = await Bytes.decode(sock)
        return ODBRecord(r_type, r_id, r_version, r_content)

class Introduction:

    @staticmethod
    def encode(_):
        return String.encode(NAME) + String.encode(VERSION) + Short.encode(SUPPORTED_PROTOCOL)


class RequestHeader:

    @staticmethod
    def encode(arg):
        header = Byte.encode(chr(arg[0])) + Integer.encode(arg[1])
        if len(arg) is 2:
            return header
        return header + Bytes.encode(arg[2])


class ODBSignal:

    def __init__(self):
        self._subscribers = []

    def __call__(self, coro, *args, **kwargs):
        assert inspect.isawaitable(coro), \
            "First argument must be awaitable, e.g. coroutine or future."
        self._subscribers.append((coro, args, kwargs))

    async def send(self, handler):
        await asyncio.gather(
            *(sub[0](handler, *sub[1], **sub[2])
              for sub in self._subscribers)
        )


class BaseHandler(AsyncObject):

    def __init__(self, client, *args):
        super().__init__(loop=client._loop)
        self._sent = asyncio.Event(loop=self._loop)
        self._client = client
        self._sock = client._sock
        self._request = b''.join(
            field_type.encode(value)
            for field_type, value in args
        )
        self.before_send = ODBSignal()
        self.before_read = ODBSignal()
        self.on_done = ODBSignal()
        self.response = None

    async def _on_done(self):
        await self._done.wait()
        self.on_done.send(self)

    async def send(self):
        await self.before_send.send(self)
        await self._sock.send(self._request)
        self._sent.set()
        return self

    async def read_header(self, with_token: bool = True):
        await self._sent.wait()
        await self.before_read.send(self)
        status = await Byte.decode(self._sock)
        s_id = await Integer.decode(self._sock)
        auth_token = b''
        if with_token:
            auth_token = await Bytes.decode(self._sock)
        return ODBResponseHeader(
            status, s_id, auth_token
        )

    async def read_clusters(self):
        for _ in range(await Short.decode(self._sock)):
            yield ODBCluster(await String.decode(self._sock), await Short.decode(self._sock))
