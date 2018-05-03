import asyncio
import inspect
from typing import Callable

from aio_pyorient.message.base import (
    BaseHandler
)
from aio_pyorient.message.constants import QUERY_ASYNC, QUERY_CMD
from aio_pyorient.odb_types import ODBRecord
from aio_pyorient.message.encoder import Bytes, Char, String, Integer, RequestHeader


class Query(BaseHandler):
    _callback = None
    results = None
    def __init__(self,
                 client,
                 query: str, *,
                 command_type: str= QUERY_CMD,
                 limit: int=25,
                 fetch_plan: str='*:0',
                 mode: str='s',
                 callback: Callable=None,
                 **kwargs):

        if mode == 'a':
            self.results = asyncio.Queue(loop=self._loop)
            command_type = QUERY_ASYNC
            if not inspect.iscoroutinefunction(callback):
                raise ValueError(
                    """
                    Query needs a coroutine function as callback 
                    when mode set to 'a'!
                    """
                )
            self._callback = callback
        self._mode = mode
        if "LIMIT" in query.upper():
            limit = -1
        payload = b''.join([
            String(command_type),
            String(query),
            Integer(limit),
            String(fetch_plan),
            Integer(0)
        ])
        super().__init__(
            client,
            (RequestHeader, (41, client._session_id, client._auth_token)),
            (Char, mode),
            (Bytes, payload),
            **kwargs
        )

    async def read_id(self):
        c_id, pos = [await self.read_short(), await self.read_long()]
        return f"#{c_id}:{pos}"

    async def read_record(self):
        r_type = await self.read_char()
        r_id = await self.read_id()
        r_version = await self.read_int()
        r_content = await self.read_bytes()
        return ODBRecord(r_type, r_id, r_version, r_content)

    async def read_next(self):
        marker = await self.read_short()
        if marker is -2:
            return None
        if marker is -3:
            return await self.read_id()
        return await self.read_record()

    async def read_records_async(self):
        while True:
            status = await self.read_byte()
            if status is 0:
                break
            record = await self.read_next()
            if status is 1:
                await self.results.put(record)
                await self._callback(record)
        return self.results

    async def _read(self):
        await self.read_header()
        if self._mode == 'a':
            return await self.read_records_async()
        records = ()
        result_type = await self.read_char()
        if result_type == 'n':
            await self.read_char()
        elif result_type in 'rw':
            records = (await self.read_next(),)
            await self.read_char()
            if result_type == 'w':
                records = (
                    records[0].data.decode().replace('result:', ''),
                )
        elif result_type == 'l':
            _len = await self.read_int()
            for _ in range(_len):
                records += (await self.read_next(),)
        elif result_type == 'i':
            records = await self.read_records_async()
        return (record for record in records)
