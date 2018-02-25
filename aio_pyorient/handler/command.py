import inspect
from typing import Callable

from aio_pyorient.handler.base import (
    BaseHandler, Byte, Bytes, Char, Integer, Link, Record, RequestHeader, Short, String
)
from aio_pyorient.otypes import OrientRecord, OrientRecordLink


QUERY_SYNC    = "com.orientechnologies.orient.core.sql.query.OSQLSynchQuery"
QUERY_ASYNC   = "com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery"
QUERY_CMD     = "com.orientechnologies.orient.core.sql.OCommandSQL"
QUERY_GREMLIN = "com.orientechnologies.orient.graph.gremlin.OCommandGremlin"
QUERY_SCRIPT  = "com.orientechnologies.orient.core.command.script.OCommandScript"



class QueryCommand(BaseHandler):

    def __init__(self,
                 client,
                 session_id: int,
                 auth_token: bytes,
                 query: str,
                 command_type: str=QUERY_CMD,
                 limit: int=25,
                 fetch_plan: str='*:0',
                 mode: str='s',
                 callback: Callable=None):
        if mode == 'a':
            self.check_callback(callback)
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
            (RequestHeader, (41, session_id, auth_token)),
            (Char, mode),
            (Integer, len(payload)),
            (Bytes, payload)
        )
        self._callback = callback

    @staticmethod
    def check_callback(callback):
        if callback is None:
            raise ValueError("QueryCommand needs a callback when mode set to 'a'!")
        is_func = inspect.isfunction(callback)
        is_coro = inspect.iscoroutinefunction(callback)
        if not is_func and not is_coro:
            raise ValueError(
                "QueryCommand callback must be a coroutine or a function!"
            )

    async def read_record(self):
        marker = await Short.decode(self._sock)
        if marker is -2:
            return None
        if marker is -3:
            return OrientRecordLink(await Link.decode(self._sock))
        record = await Record.decode(self._sock)
        class_name, data = self._serializer.decode(record.content.rstrip())
        return OrientRecord(
            dict(
                __o_storage=data,
                __o_class=class_name,
                __version=record.version,
                __rid=record.id
            )
        )

    async def read_records_async(self):
        records = []
        cached = []
        while True:
            status = await Byte.decode(self._sock)
            if status is 0:
                break
            record = await self.read_record()
            if status is 1:
                records.append(record)
                if inspect.isfunction(self._callback):
                    self._callback(record)
                else:
                    await self._callback(record)
            if status is 2:
                cached.append(await self.read_record())
        return records, cached

    async def _read(self):
        records, prefetched = (), None
        s_id, auth_token = await self.read_header()
        result_type = await Char.decode(self._sock)
        if result_type == 'n':
            await Char.decode(self._sock)
        elif result_type in 'rw':
            records = (await self.read_record(),)
            await Char.decode(self._sock)
            if result_type == 'w':
                records = (
                    self._serializer.decode(records[0].oRecordData['result']),
                )
        elif result_type == 'l':
            _len = await Integer.decode(self._sock)
            for _ in range(_len):
                records += (await self.read_record(),)
            prefetched = await self.read_records_async()
        elif result_type == 'i':
            records, cached = await self.read_records_async()
        return self.response_type(s_id, auth_token, records, prefetched)
