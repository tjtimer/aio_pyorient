import struct
from collections import namedtuple

ODBRecord = namedtuple("ODBRecord", "type, id, version, data")
ODBCluster = namedtuple('ODBCluster', 'name, id')
ODBRequestErrorMessage = namedtuple("ODBException", "class_name, message")


int_packer = struct.Struct("!i")
short_packer = struct.Struct("!h")
long_packer = struct.Struct("!q")


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
        decoded = long_packer.unpack(await sock.recv(8))[0]
        return decoded

class ODBClusters(list):

    def get(self, prop: int or str)->str or int:
        cluster = [cl for cl in self if prop in cl][0]
        if isinstance(prop, int):
            return cluster.name
        return cluster.id
