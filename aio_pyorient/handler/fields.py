import struct

int_packer = struct.Struct("!i")
short_packer = struct.Struct("!h")
long_packer = struct.Struct("!q")


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
