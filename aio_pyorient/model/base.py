"""

 base
"""
from aio_pyorient.model.prop_types import String, Integer, PropType
from aio_pyorient.utils import AsyncCtx


InitCommands = []
vertex_registry = []
edge_registry = []


class CommandBuilder:

    @classmethod
    def _props(cls):
        return {k: v for k, v in cls.__dict__.items()
                if isinstance(v, PropType)}

    @classmethod
    def prop_cmds(cls, alter_or_create: str='alter'):

        for name, prop_type in cls._props().items():
            definition = f"{cls.__name__}.{name} {prop_type}"
            yield f"{alter_or_create.upper()} PROPERTY {definition}"


class ODBVertex(CommandBuilder):
    def __init_subclass__(cls):
        vertex_registry.append(cls)

    @property
    def props(self):
        return self._props()

class ODBEdge(CommandBuilder):
    def __init_subclass__(cls):
        edge_registry.append(cls)
        InitCommands.append(
            f"CREATE PROPERTY {cls.__name__}.label (MANDATORY True)"
        )
        cls.label = cls.__name__.lower()
