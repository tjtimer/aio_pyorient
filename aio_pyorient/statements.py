# statements
from typing import Iterator


def select(model: str, *, fields: str or Iterator='*') -> str:
    return f"Select {', '.join(f'`{model}`.`{field}`' for field in fields)} FROM `{model}`"
