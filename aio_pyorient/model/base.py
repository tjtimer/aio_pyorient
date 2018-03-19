"""

 base
"""
from aio_pyorient.model.prop_types import String, Integer, PropType
from aio_pyorient.utils import AsyncCtx


InitCommands = []
vertex_registry = []
edge_registry = []


class CommandBuilder:

    @staticmethod
    async def props_cmd(cls, alter_or_create: str='alter'):

        def props(cls):
            return ((k, v) for k, v in cls.__dict__.items() if isinstance(k, PropType))

        def add_attr_definition(**attributes):
            attr_command = ', '.join([
                f"{attr_name} {attr_value}"
                for attr_name, attr_value in attributes
            ])
            return f' ({attr_command})'

        for name, prop_type in props(cls):
            prop_command = f"""
            {alter_or_create.upper()} PROPERTY
            {cls.__name__}.{name} {prop_type.__class__.__name__}
            """.strip().replace('\n', '')
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
