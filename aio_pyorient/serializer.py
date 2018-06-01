"""

 serializer
"""
import re

from .schema.prop_types import TYPE_MAP


key_reg = re.compile(r',?[a-zA-Z0-9_\-]+:')
float_reg = re.compile(r'-?[0-9]+\.?[0-9]*')
int_reg = re.compile(r'-?[0-9]+')
str_reg = re.compile(r'[a-zA-Z]+')

Key = lambda v: key_reg.search(v).group(0)
String = lambda v: str_reg.search(v).group(0) if len(v) > 0 else v
Integer = lambda v: int(int_reg.search(v).group(0)) if len(v) else None
Float = lambda v: float(v[:-1] if 'f' in v else v)
Boolean = lambda v: True if v.upper() == 'TRUE' else False
List = lambda v: v[1:-1].split(', ') if len(v) > 1 else []
FloatList = lambda v: list(float(x) for x in float_reg.findall(v))
IntegerList = lambda v: list(int(x) for x in int_reg.findall(v))
StringList = lambda v: list(x for x in str_reg.findall(v))
Type = lambda v: TYPE_MAP[int(v)]

def get_key_value_pairs(data, matches, index, specs):
    key = String(matches[index].group(0))
    if index is -1:
        value = data[matches[index].end():]
    else:
        value = data[matches[index].end():matches[index + 1].start()]
    try:
        value = specs[key](value)
    except KeyError:
        pass
    return (key, value)

def serialize(data: str, specs: dict)->dict:
    matches = list(key_reg.finditer(data))
    key_count = len(matches)
    if key_count:
        pairs = []
        for i in range(key_count - 1):
            pairs.append(get_key_value_pairs(data, matches, i, specs))
        pairs.append(get_key_value_pairs(data, matches, -1, specs))
        return dict(pairs)
    return dict()

