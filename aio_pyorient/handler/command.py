import asyncio
import inspect
from typing import Callable

from aio_pyorient.handler.base import (
    BaseHandler, RequestHeader
)
from aio_pyorient.odb_types import Bytes, Char, String, Integer, ODBRecord


QUERY_SYNC    = "com.orientechnologies.orient.core.sql.query.OSQLSynchQuery"
QUERY_ASYNC   = "com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery"
QUERY_CMD     = "com.orientechnologies.orient.core.sql.OCommandSQL"
QUERY_GREMLIN = "com.orientechnologies.orient.graph.gremlin.OCommandGremlin"
QUERY_SCRIPT  = "com.orientechnologies.orient.core.command.script.OCommandScript"



class QueryCommand(BaseHandler):
    _callback = None
    results = None
    def __init__(self,
                 client,
                 query: str,
                 command_type: str=QUERY_CMD,
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
                    QueryCommand needs a coroutine function as callback 
                    when mode set to 'a'!
                    """
                )
            self._callback = callback
        self._mode = mode
        if "LIMIT" in query.upper():
            limit = -1
        payload = b''.join([
            String.encode(command_type),
            String.encode(query),
            Integer.encode(limit),
            String.encode(fetch_plan),
            Integer.encode(0)
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
                    records[0].data.decode().replace('result', ''),
                )
        elif result_type == 'l':
            _len = await self.read_int()
            for _ in range(_len):
                records += (await self.read_next(),)
        elif result_type == 'i':
            records = await self.read_records_async()
        return (record for record in records)
