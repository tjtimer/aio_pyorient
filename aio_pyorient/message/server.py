from aio_pyorient.message.base import BaseHandler
from aio_pyorient.message.encoder import Boolean, String, Introduction, RequestHeader


class ServerConnect(BaseHandler):

    def __init__(
            self, client, user: str, password: str, *,
            use_token_auth: bool = True,
            support_push: bool = True,
            collect_stats: bool = True,
            **kwargs):
        super().__init__(
            client,
            (RequestHeader, (2, -1)),
            (Introduction, None),
            (String, client._id),
            (String, client._serialization_type),
            (Boolean, use_token_auth),
            (Boolean, support_push),
            (Boolean, collect_stats),
            (String, user),
            (String, password),
            **kwargs
        )

    async def _read(self):
        print('ServerConnect starts reading.')
        await self.read_header(with_token=False)  # returns status, old session_id, empty byte
        self._client._session_id = await self.read_int()
        self._client._auth_token = await self.read_bytes()
        return self._client
