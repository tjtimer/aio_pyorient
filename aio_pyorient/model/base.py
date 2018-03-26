"""

 base
"""
from aio_pyorient.model.prop_types import props


InitCommands = []
vertex_registry = []
edge_registry = []


class ODBVertex:
    def __init_subclass__(cls):
        cls._name = cls.__name__
        vertex_registry.append(cls)

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            self.__setattr__(k, v)

    def __setattr__(self, name, value):
        print("setting attribute: ", name, value)
        self.__dict__[name] = value

    @property
    def props(self):
        return dict(props(self.__class__))

class ODBEdge:
    def __init_subclass__(cls):
        edge_registry.append(cls)
        cls.label = cls.__name__.lower()
