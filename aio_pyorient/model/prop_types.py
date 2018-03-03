"""

 prop_types
"""

class PropType:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.__setattr__(k, v)

class Boolean(PropType):
    pass

class Integer(PropType):
    pass

class Short(PropType):
    pass

class Long(PropType):
    pass

class Float(PropType):
    pass

class Double(PropType):
    pass

class DateTime(PropType):
    pass

class String(PropType):
    pass

class Binary(PropType):
    pass

class Embedded(PropType):
    pass

class EmbeddedList(PropType):
    pass

class EmbeddedSet(PropType):
    pass

class EmbeddedMap(PropType):
    pass

class Link(PropType):
    pass

class LinkList(PropType):
    pass

class LinkSet(PropType):
    pass

class LinkMap(PropType):
    pass


class Byte(PropType):
    pass

class Transient(PropType):
    pass

class Date(PropType):
    pass

class Custom(PropType):
    pass

class Decimal(PropType):
    pass

class LinkBag(PropType):
    pass

class Any(PropType):
    pass
