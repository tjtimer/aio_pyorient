"""
prop_types
"""
from typing import Any

PROPERTY_ATTRIBUTES = {
    'LINKEDTYPE': str,
    'LINKEDCLASS': str,
    'MIN': int,
    'MANDATORY': bool,
    'MAX': int,
    'NAME': str,
    'NOTNULL': bool,
    'REGEX': str,
    'TYPE': str,
    'COLLATE': str,
    'READONLY': bool,
    'CUSTOM': str,
    'DEFAULT': Any
}

class PropType:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            attr = str(k).upper()
            assert attr in PROPERTY_ATTRIBUTES.keys(), \
                f'{k} is not a valid attribute'
            assert isinstance(v, PROPERTY_ATTRIBUTES[attr]), \
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
    pass

class EmbeddedSet(PropType):
    pass

class Float(PropType):
    pass

class Integer(PropType):
    pass

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
    pass

class Short(PropType):
    pass

class String(PropType):
    pass

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
