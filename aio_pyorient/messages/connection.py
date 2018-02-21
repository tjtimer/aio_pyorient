from aio_pyorient.constants import (CONNECT_OP, FIELD_BOOLEAN, FIELD_BYTE, FIELD_INT, FIELD_SHORT, FIELD_STRING,
                                    FIELD_STRINGS, NAME, SHUTDOWN_OP, SUPPORTED_PROTOCOL, VERSION)


class Message:

    def __init__(self, ):

        self._user = ''
        self._pass = ''
        self._client_id = ''
        self._need_token = False
        self._append( ( FIELD_BYTE, CONNECT_OP ) )

    def prepare(self, params=None):

        if isinstance(params, (tuple, list)):
            try:
                self._user = params[0]
                self._pass = params[1]
                self._client_id = params[2]
            except IndexError:
                pass

        self._append( ( FIELD_STRINGS, [NAME, VERSION] ) )
        self._append( ( FIELD_SHORT, SUPPORTED_PROTOCOL ) )

        self._append( ( FIELD_STRING, self._client_id ) )

        if self.protocol > 21:
            self._append((FIELD_STRING, self._connection.serialization_type))
            if self.protocol > 26:
                self._append( ( FIELD_BOOLEAN, self._request_token ) )
                if self.protocol > 32:
                    self._append(( FIELD_BOOLEAN, True ))  # support-push
                    self._append(( FIELD_BOOLEAN, True ))  # collect-stats

        self._append( ( FIELD_STRING, self._user ) )
        self._append( ( FIELD_STRING, self._pass ) )
        super().prepare()
        return self

    async def fetch_response(self):

        self._append( FIELD_INT )
        if self.protocol > 26:
            self._append( FIELD_STRING )
        session_id, token = await super().fetch_response()

        self._connection.session_id = session_id

        if self.protocol > 26:
            if token is None:
                self._request_token = False

            self.set_session_token(token)

        return self.session_id


class ShutdownMessage(BaseMessage):

    def __init__(self, _orient_socket ):
        super().__init__(_orient_socket)

        self._user = ''
        self._pass = ''

        # order matters
        self._append( ( FIELD_BYTE, SHUTDOWN_OP ) )

    def prepare(self, params=None):

        if isinstance( params, tuple ) or isinstance( params, list ):
            try:
                self._user = params[0]
                self._pass = params[1]
            except IndexError:
                # Use default for non existent indexes
                pass
        self._append( (FIELD_STRINGS, [self._user, self._pass]) )
        return super().prepare()

    def set_user(self, _user):
        self._user = _user
        return self

    def set_pass(self, _pass):
        self._pass = _pass
        return self
