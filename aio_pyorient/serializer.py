"""

 serializer
"""
import io
import re

from aio_pyorient.handler.base import int_packer


TYPE_MAP = {
    '1': 'Boolean',
    '2': 'Byte',
    '3': 'Short',
    '4': 'Integer',
    '5': 'Long',
    '6': 'Bytes',
    '7': 'String',
    '8': 'Record',
    '9': 'Strings',
    '10': 'Char',
    '11': 'Link'
}
int_reg = re.compile(r'-?\d*')
Key = lambda v: v.replace(',', '').replace(':', '').replace('@', '')
String = lambda v: v[1:-1] if len(v) > 1 else v
Integer = lambda v: int(v.replace('"', '')) if len(v) > 0 else None
Float = lambda v: float(v[:-1])
Boolean = lambda v: True if v.upper() == 'TRUE' else False
List = lambda v: v[1:-1].split(',') if len(v) > 1 else []
IntegerList = lambda v: [int(val) for val in List(v)]
StringList = lambda v: [val.replace('"', '') for val in List(v)]
Type = lambda v: TYPE_MAP[v]

key_reg = re.compile(r',?@?[a-zA-Z]+:')
def csv_serialize(data: str, specs: dict)->dict:
    data.replace('"', '')
    matches = list(key_reg.finditer(data))
    key_count = len(matches)
    keys = []
    values = []
    if key_count:
        for i in range(key_count - 1):
            key = Key(matches[i].group(0))
            value = data[matches[i].end():matches[i + 1].start()]
            keys.append(key)
            try:
                values.append(specs[key](value))
            except KeyError:
                values.append(value)
        key = Key(matches[-1].group(0)[1:-1])
        keys.append(key)
        value = data[matches[-1].end() + 1:]
        try:
            values.append(specs[key](value))
        except KeyError:
            values.append(value)
    return dict(zip(keys, values))



def binary_serialize(data, result: dict={}):
    buffer = io.BytesIO(data)
    offset = 0
    length = int(ord(data[offset]))
    offset += 1
    end = offset+length
    class_name = data[offset:end].encode()
    offset = end
    end = offset + 4
    position = int_packer.unpack('>i', data[offset:end])
    offset = offset + end
    data_type = int(ord(data[offset]))
