import struct

from aio_pyorient.sock import OrientSocket


class FieldDefinitionError(Exception):
    """If something is wrong with attributes set on the field."""


class BaseField:
    __slots__ = ('_name', 'encoded', 'decoded')
    bytes = None
    struct = None

    @classmethod
    def __init_subclass__(cls):
        if not cls.bytes and not cls.struct:
            raise FieldDefinitionError(
                f"""
                Attributes 'struct' and 'bytes' may not be `None` at the same time.
                \nField: {cls.__name__}
                """
            )
        if cls.bytes and cls.struct:
            raise FieldDefinitionError(
                f"""
                Attributes 'struct' and 'bytes' may not be set at the same time.
                \nField: {cls.__name__}
                """
            )

    def __init__(self, *, sock: OrientSocket = None):
        self._sock = sock

    async def read(self):
        return await self._sock.read(self.bytes)

class Integer(BaseField):
    bytes = 4

    async def encode(self, input):
        return struct.pack(">i", input)

    async def decode(self):
        return struct.unpack(">i", await self.read())[0]
