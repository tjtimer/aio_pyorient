"""

 base
"""
from aio_pyorient.schema.prop_types import String, Integer, PropType
from aio_pyorient.utils import AsyncCtx


InitCommands = []
vertex_registry = []
edge_registry = []


class CommandBuilder:
    def __init_subclass__(cls, **kwargs):
        cls._name = cls.__name__

    @property
    def props(self):
        return {k: v
                for k, v in self.__dict__.items()
                if isinstance(k, PropType)}

    @property
    def create_props_cmd(self):
        return list(self.props_cmd('create'))

    @property
    def alter_props_cmd(self):
        return list(self.props_cmd())

    def props_cmd(self, alter_or_create: str='alter'):

        def add_attr_definition(**attributes):
            attr_command = ', '.join([
                f"{attr_name} {attr_value}"
                for attr_name, attr_value in attributes
            ])
            return f' ({attr_command})'

        for name, prop_type in self.props:
            prop_command = (
                f"{alter_or_create.upper()} PROPERTY"
                f"{self._name}.{name} {prop_type.__class__.__name__}"
            )
            prop_command += add_attr_definition(**prop_type.__dict__)
            yield prop_command

class ODBVertex(CommandBuilder):
    def __init_subclass__(cls):
        vertex_registry.append(cls)

class ODBEdge(CommandBuilder):
    def __init_subclass__(cls):
        edge_registry.append(f"CREATE CLASS {cls.__name__} EXTENDS E")
        InitCommands.append(
            f"CREATE PROPERTY {cls.__name__}.label (MANDATORY True)"
        )
        cls.label = cls.__name__.lower()
