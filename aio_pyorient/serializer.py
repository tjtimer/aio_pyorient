"""

 serializer
"""
import re

from .schema.prop_types import TYPE_MAP


key_reg = re.compile(r',?@?[a-zA-Z0-9_\-]+:')
float_reg = re.compile(r'-?[0-9]+\.?[0-9]*')
int_reg = re.compile(r'-?[0-9]+')
str_reg = re.compile(r'[a-zA-Z]+')

Key = lambda v: str_reg.search(v).group(0)
String = lambda v: v[1:-1] if len(v) > 1 else v
Integer = lambda v: int(int_reg.search(v).group(0)) if len(v) else None
Float = lambda v: float(v[:-1])
Boolean = lambda v: True if v.upper() == 'TRUE' else False
List = lambda v: v[1:-1].split(', ') if len(v) > 1 else []
FloatList = lambda v: list(float(x) for x in float_reg.findall(v))
IntegerList = lambda v: list(int(x) for x in int_reg.findall(v))
StringList = lambda v: list(x for x in str_reg.findall(v))
Type = lambda v: TYPE_MAP[int(v)]


def serialize(data: str, specs: dict)->dict:
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

