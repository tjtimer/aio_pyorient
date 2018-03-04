import re
import json
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
        return long_packer.pack(value)

    @staticmethod
    async def decode(sock):
        decoded = long_packer.unpack(await sock.recv(8))[0]
        return decoded

class ODBClusters(list):

    def get(self, prop: int or str)->str or int:
        is_id = isinstance(prop, int)
        attr_type = 'id' if is_id else 'name'
        try:
            cluster = [cl for cl in self if prop in cl][0]
        except IndexError:
            raise ValueError(
                f"cluster with {attr_type} {prop} does not exist"
            )
        else:
            if is_id:
                return cluster.name
            return cluster.id

key_reg = re.compile(r',?[a-zA-Z]+:')
class Schema:
    def __init__(self, record_generator):
        self._classes = {}
        for record in record_generator:
            data=record.data.decode()
            matches = list(key_reg.finditer(data))
            key_count = len(matches)
            keys = []
            values = []
            if key_count:
                prop_keys = []
                prop_values = []
                props = []
                in_props = False
                for i in range(key_count-1):
                    key = matches[i].group(0)[0:-1].replace(
                        ',', '').replace('"', '')
                    value = data[matches[i].end():matches[i+1].start()].replace('"', '')
                    if value == '<(':
                        keys.append(key)
                        in_props = True
                        continue
                    if ')>' in value:
                        prop_keys.append(key)
                        prop_values.append(value.replace(')>', ''))
                        props.append(dict(zip(prop_keys, prop_values)))
                        values.append({prop['name']:prop for prop in props})
                        in_props = False
                        continue
                    if in_props is True:
                        if key in prop_keys:
                            props.append(dict(zip(prop_keys, prop_values)))
                            prop_keys.clear()
                            prop_values.clear()
                        prop_keys.append(key)
                        prop_values.append(value.replace('),(', ''))
                    else:
                        keys.append(key)
                        values.append(value)
                keys.append(matches[-1].group(0)[1:-1])
                values.append(data[matches[-1].end()+1:])
            serialized = dict(zip(keys, values))
            self._classes[serialized['name']] = serialized

    @property
    def classes(self):
        return self._classes

    def __str__(self):
        return f'<Schema {sorted(list(self._classes.keys()))}>'
