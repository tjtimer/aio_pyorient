import asyncio
from collections import deque

from aio_pyorient import ODBClient
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
        self._available_clients = deque()
        self._busy_clients = deque()
        self._is_full = asyncio.Event(loop=self._loop)
        self._kwargs = kwargs

    @property
    def min(self):
        return self._min

    @property
    def max(self):
        return self._max

    @property
    def is_full(self):
        return self._is_full.is_set()

    @property
    def available_clients(self):
        return len(self._available_clients)

    @property
    def busy_clients(self):
        return len(self._busy_clients)

    @property
    def clients(self):
        return len(self._available_clients) + len(self._busy_clients)

    async def acquire(self):
        try:
            await self._is_ready.wait()
            client = self._available_clients.popleft()
            client._is_ready.clear()
            self._busy_clients.append(client)
            self.spawn(self._watch_client, client)
            return client
        finally:
            if self.available_clients is 0:
                self._is_ready.clear()
                self.spawn(self._add_client)

    async def _add_client(self):
        if self.cancelled or self.clients >= self._max:
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
        self._available_clients.append(client)
        self._is_ready.set()
        return self

    async def _watch_client(self, client):
        try:
            await client._is_ready.wait()
            self._busy_clients.remove(client)
            if not self.cancelled:
                self._available_clients.append(client)
                self._is_ready.set()
            return self._done.set()
        except asyncio.CancelledError:
            print("pool._watch_client got cancelled")
            raise asyncio.CancelledError()


    async def setup(self):
        await asyncio.gather(
            *(self._add_client() for _ in range(self._min))
        )
        return self

    async def _shutdown(self, *args, **kwargs):
        while True:
            if len(self._available_clients) <= 0:
                if len(self._busy_clients) <= 0:
                    break
                client = self._busy_clients.pop()
                await client.shutdown()
            else:
                client = self._available_clients.pop()
                await client.shutdown()
