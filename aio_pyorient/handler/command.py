import inspect
from typing import Callable

from aio_pyorient.handler.base import (
    BaseHandler, Byte, Bytes, Char, Integer, Link, Record, RequestHeader, Short, String,
    Boolean
)
from aio_pyorient.otypes import OrientRecord, OrientRecordLink


QUERY_SYNC    = "com.orientechnologies.orient.core.sql.query.OSQLSynchQuery"
QUERY_ASYNC   = "com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery"
QUERY_CMD     = "com.orientechnologies.orient.core.sql.OCommandSQL"
QUERY_GREMLIN = "com.orientechnologies.orient.graph.gremlin.OCommandGremlin"
QUERY_SCRIPT  = "com.orientechnologies.orient.core.command.script.OCommandScript"



class QueryCommand(BaseHandler):
    _callback = None
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
            (RequestHeader, (41, client._session_id, client._auth_token)),
            (Char, mode),
            (Bytes, payload),
            **kwargs
        )
        self._mode = mode

    def check_callback(self, callback):
        if not inspect.iscoroutinefunction(callback):
            raise ValueError(
                "QueryCommand needs a coroutine function as callback when mode set to 'a'!"
            )
        self._callback = callback

    async def read_record(self):
        marker = await self.read_short()
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
        records = ()
        while True:
            status = await self.read_byte()
            if status is 0:
                break
            record = await self.read_record()
            if status is 1:
                records += (record,)
                await self._callback(record)
        return records

    async def _read(self):
        await self.read_header()
        if self._mode == 'a':
            return await self.read_records_async()
        records = ()
        result_type = await self.read_char()
        if result_type == 'n':
            await self.read_char()
        elif result_type in 'rw':
            records = (await self.read_record(),)
            await self.read_char()
            if result_type == 'w':
                records = (
                    self._serializer.decode(records[0].oRecordData['result']),
                )
        elif result_type == 'l':
            _len = await self.read_int()
            for _ in range(_len):
                records += (await self.read_record(),)
        elif result_type == 'i':
            records, _ = await self.read_records_async()
        return records
