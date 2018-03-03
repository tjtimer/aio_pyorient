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
            assert str(k) in PROPERTY_ATTRIBUTES.keys(), \
                f'{k} is not a valid attribute'
            assert isinstance(v, PROPERTY_ATTRIBUTES[str(k)]), \
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
