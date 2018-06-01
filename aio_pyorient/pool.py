import asyncio

from aio_pyorient.client import ODBClient
from aio_pyorient.utils import AsyncCtx


class ODBPool(AsyncCtx):
    def __init__(self,
                 user: str, password:str, *,
                 min: int=5, max: int=-1, db_name:str=None, **kwargs):
        super().__init__(**kwargs)
        self.__user, self.__password = user, password
        self._db_name = db_name
        self._min = min
        if max <= min:
            max = 30000
        self._max = max
        self._clients = asyncio.Queue()
        self._client_count = 0
        self._kwargs = kwargs

    @property
    def min(self):
        return self._min

    @property
    def max(self):
        return self._max

    @property
    def available_clients(self):
        if self._clients is None:
            return 0
        return self._clients.qsize()

    @property
    def size(self):
        return self._client_count

    async def acquire(self):
        try:
            await self._is_ready.wait()
            client = await self._clients.get()
            client._is_ready.clear()
            self.spawn(self._watch_client(client))
            return client
        finally:
            if self.available_clients is 0:
                self._is_ready.clear()
                self.spawn(self._add_client())

    async def _add_client(self):
        if self.cancelled or self.size >= self._max:
            return
        client = ODBClient(**self._kwargs)
        if self._db_name is None:
            await client.connect(
                self.__user, self.__password, **self._kwargs
            )
        else:
            await client.open_db(
                self._db_name, self.__user, self.__password, **self._kwargs
            )
        await client._is_ready.wait()
        self._clients.put_nowait(client)
        self._client_count += 1
        self._is_ready.set()
        return self

    async def _watch_client(self, client):
        try:
            await client._is_ready.wait()
            if not self.cancelled and self._client_count<=self._max:
                self._clients.put_nowait(client)
                self._is_ready.set()
            else:
                await client.shutdown()
                self._client_count -= 1
        except Exception as e:
            pass

    async def setup(self):
        await self.wait_for(
            *(self._add_client() for _ in range(self._min))
        )
        return self

    async def _shutdown(self, *args, **kwargs):
        self._clients = None
