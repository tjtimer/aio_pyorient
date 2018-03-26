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
    _has_attributes = False
    def __init__(self, **kwargs):
        self._name = self.__class__.__name__
        if len(kwargs):
            for k, v in kwargs.items():
                self.add_attr(k, v)
            self._has_attributes = True

    @property
    def attributes(self):
        attrs = {k: v
                 for k,v in self.__dict__.items()
                 if k in PROPERTY_ATTRIBUTES.keys()}
        return attrs if len(attrs) >= 1 else None

    def __repr__(self):
        if self.attributes is None:
            return self._name
        a_str = f"{', '.join(f'{k} {v}' for k,v in self.attributes.items())}"
        return f"{self._name} ({a_str})"

    def add_attr(self, name, value):
        attr = str(name).upper()
        assert attr in PROPERTY_ATTRIBUTES.keys(), \
            f'{name} is not a valid attribute'
        assert isinstance(value, PROPERTY_ATTRIBUTES[attr]), \
            f'{value} is not a valid value for attribute {name}'
        if isinstance(value, str):
            value = f'"{value}"'
        self.__setattr__(attr, value)

    def create_cmd(self, cls, name):
        assert hasattr(cls, name)
        return f"CREATE PROPERTY {cls._name}.{name} {self.__repr__()}"

    def alter_cmd(self, cls, name, attr, value):
        assert hasattr(cls, name)
        return f"ALTER PROPERTY {cls._name}.{name} {attr} {value}"

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


def props(cls):
    return ((k, v) for k, v in cls.__dict__.items() if isinstance(v, PropType))


def props_cmd(cls, alter_or_create: str='alter'):
    start = f"{alter_or_create.upper()} PROPERTY"
    for name, prop_type in props(cls):
        yield f"{start} {cls.__name__}.{name} {prop_type}"
