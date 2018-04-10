"""

 encoder
"""
from aio_pyorient.handler.base import int_packer, short_packer, long_packer, NAME, VERSION, SUPPORTED_PROTOCOL


Boolean = lambda v: bytes([1]) if v else bytes([0])

Byte = lambda v: bytes([ord(v)])

Bytes = lambda v: int_packer.pack(len(v)) + v

Char = lambda v: bytes(v, encoding="utf-8")

String = lambda v: int_packer.pack(len(v)) + bytes(v, encoding="utf-8")

Integer = lambda v: int_packer.pack(v)

Short = lambda v: short_packer.pack(v)

Long = lambda v: long_packer.pack(v)

RecordId = lambda v: b''.join([Short(int(v.split(':')[0][1:])),
                              Long(v.split(':')[1])])

Record = lambda v: b''.join([Byte(v.type.encode("utf-8")),
                            RecordId(v.id),
                            Integer(v.version),
                            Bytes(v.content)])

Introduction = lambda v: String(NAME) + String(VERSION) + Short(SUPPORTED_PROTOCOL)

RequestHeader = lambda v: b''.join(
    [Byte(chr(v[0])), Integer(v[1])]
) if len(v) is 2 else b''.join(
    [Byte(chr(v[0])), Integer(v[1]), Bytes(v[2])]
)
