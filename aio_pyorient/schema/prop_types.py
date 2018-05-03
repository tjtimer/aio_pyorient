"""
prop_types
"""
import typing
from io import BytesIO

def var_int(stream: BytesIO)->int:
    result = 0
    shift = 0
    while True:
        raw = ord(stream.read(1))
        result |= raw << shift
        if (raw & 0x80) is 0:
            break
        shift += 7
    if result % 2 is 1:
        result = -(result+1)
    return int(result/2)

class PropType:
    attr_def = {
        'COLLATE': str,
        'CUSTOM': str,
        'DEFAULT': typing.Any,
        'LINKEDTYPE': str,
        'LINKEDCLASS': str,
        'MIN': int,
        'MANDATORY': bool,
        'MAX': int,
        'NAME': str,
        'NOTNULL': bool,
        'READONLY': bool,
        'REGEX': str,
        'TYPE': str
    }
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            attr = str(k).upper()
            assert attr in self.attr_def.keys(), \
                f'{k} is not a valid attribute'
            assert isinstance(v, self.attr_def[attr]), \
                f'{v} is not a valid value for attribute {k}'
            self.__setattr__(k, v)


class Any(PropType):
    pass

class Binary(PropType):
    pass

class Boolean(PropType):
    pass

class Byte(PropType):
    pass

class Custom(PropType):
    pass

class Date(PropType):
    pass

class DateTime(PropType):
    pass

class Decimal(PropType):
    pass

class Double(PropType):
    pass

class Embedded(PropType):
    pass

class EmbeddedMap(PropType):
    pass

class EmbeddedList(PropType):

    @staticmethod
    def serialize(stream: BytesIO)->list:
        length = var_int(stream)
        item_type = TYPE_MAP[ord(stream.read(1))]
        result = []
        return result

class EmbeddedSet(PropType):
    pass

class Float(PropType):
    pass

class Integer(PropType):
    @staticmethod
    def serialize(stream: BytesIO)->int:
        value = var_int(stream)
        return value

class Link(PropType):
    pass

class LinkBag(PropType):
    pass

class LinkMap(PropType):
    pass

class LinkList(PropType):
    pass

class LinkSet(PropType):
    pass

class Long(PropType):
    @staticmethod
    def serialize(stream: BytesIO)->int:
        value = var_int(stream)
        return value

class Short(PropType):
    @staticmethod
    def serialize(stream: BytesIO)->int:
        value = var_int(stream)
        return value

class String(PropType):
    @staticmethod
    def serialize(stream: BytesIO)->str:
        length = var_int(stream)
        string = stream.read(length).decode()
        return string

class Transient(PropType):
    pass


TYPE_MAP = {
    0: Boolean,
    1: Integer,
    2: Short,
    3: Long,
    4: Float,
    5: Double,
    6: DateTime,
    7: String,
    8: Binary,
    9: Embedded,
    10: EmbeddedList,
    11: EmbeddedSet,
    12: EmbeddedMap,
    13: Link,
    14: LinkList,
    15: LinkSet,
    16: LinkMap,
    17: Byte,
    18: Transient,
    19: Date,
    20: Custom,
    21: Decimal,
    22: LinkBag,
    23: Any
}
