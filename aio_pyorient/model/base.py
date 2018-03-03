"""

 base
"""
from aio_pyorient.model.prop_types import String, Integer
from aio_pyorient.utils import AsyncCtx


InitCommands = []

class Vertex:
    def __init_subclass__(cls):
        InitCommands.append(f"CREATE CLASS {cls.__name__} EXTENDS V")
        props = {k: v for k,v in cls.__dict__.items() if not k.startswith('_')}
        for name, prop_type in props.items():
            prop_command = f"""
            CREATE PROPERTY {cls.__name__}.{name} {prop_type.__class__.__name__}
            """.strip().replace('\n', '')
            if len(prop_type.__dict__):
                prop_command += "("
                for prop_attr, attr_value in prop_type.__dict__.items():
                    prop_command += f" {prop_attr} {attr_value},"
                prop_command = prop_command[0:-1]
                prop_command += ")"
            InitCommands.append(prop_command)

class Edge:
    def __init_subclass__(cls):
        InitCommands.append(f"CREATE CLASS {cls.__name__} EXTENDS E")
        InitCommands.append(
            f"CREATE PROPERTY {cls.__name__}.label (MANDATORY True)"
        )
        cls.label = cls.__name__.lower()

class Graph(AsyncCtx):
    def __init__(self, db_name: str, user: str, password: str, **kwargs):
        self._db_name = db_name
        self._user = user
        self._password = password
