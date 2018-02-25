import asyncio

from aio_pyorient.utils import AsyncBase


class ODBPool(AsyncBase):
    _clients = []
    min: int  # minimum clients in pool - default is 5
    max: int  # maximum clients in pool - default -1 meaning no limit
    def __init__(self, *, min: int=5, max: int=-1, **kwargs):
        super().__init__(**kwargs)
        self._max_reached = asyncio.Event(loop=self._loop)
        self.min = min
        self.max = max
