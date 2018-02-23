from collections import namedtuple

from aio_pyorient.handler.base import BaseHandler, Boolean, Bytes, Integer, Introduction, RequestHeader, String
from aio_pyorient.serializations import OrientSerialization


class Connect(BaseHandler):
    _result = namedtuple("ServerConnectResponse", "session_id, auth_token")

    def __init__(
            self, client, user: str, password: str, *,
            client_id: str = '',
            serialization_type: OrientSerialization = OrientSerialization.CSV,
            use_token_auth: bool = True,
            support_push: bool = True,
            collect_stats: bool = True):
        super().__init__(
            client,
            (RequestHeader, (2, -1)),
            (Introduction, None),
            (String, client_id),
            (String, serialization_type),
            (Boolean, use_token_auth),
            (Boolean, support_push),
            (Boolean, collect_stats),
            (String, user),
            (String, password)
        )

    async def read(self):
        await self.read_header(with_token=False)  # returns status, old session_id, empty byte
        _result = [
            await Integer.decode(self._sock), await Bytes.decode(self._sock)
        ]
        return self._result(*_result)
