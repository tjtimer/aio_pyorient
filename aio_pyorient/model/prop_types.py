"""

 prop_types
"""

class PropType:
    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.__setattr__(k, v)

class String(PropType):
    pass

class Integer(PropType):
    pass
