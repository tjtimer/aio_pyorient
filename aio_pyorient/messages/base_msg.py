import asyncio
import struct
import sys

from aio_pyorient.exceptions import PyOrientBadMethodCallException, \
    PyOrientCommandException, PyOrientNullRecordException
from aio_pyorient.otypes import OrientRecord, OrientRecordLink, OrientNode

from aio_pyorient.hexdump import hexdump
from aio_pyorient.constants import BOOLEAN, BYTE, BYTES, CHAR, FIELD_BOOLEAN, FIELD_BYTE, \
    FIELD_INT, FIELD_RECORD, FIELD_SHORT, FIELD_STRING, FIELD_TYPE_LINK, INT, \
    LINK, LONG, RECORD, SHORT, STRING, STRINGS
from aio_pyorient.sock import OrientSocket
from aio_pyorient.serializations import OrientSerialization

class BaseMessage(object):
    _name = None

    def __init_subclass__(cls, *args, **kwargs):
        cls._name = cls.__name__

    def __init__(self, sock=OrientSocket(), *,
                 loop: asyncio.AbstractEventLoop = asyncio.get_event_loop()):
        """
        :type sock: OrientSocket
        """
        self._loop = loop
        self._connection = sock
        self._ready = asyncio.Event(loop=self._loop)
        self._request_token = True
        self._header = []
        self._body = []
        self._fields_definition = []
        self._command = chr(0)
        self._node_list = []
        self._output_buffer = b''
        self._input_buffer = b''
        self._callback = None
        self._push_callback = None
        self._serializer = None
        self._need_token = True

        global in_transaction
        in_transaction = False

    @property
    def _protocol(self):
        return self._connection.protocol

    @property
    def _session_id(self):
        return self._connection.session_id

    @property
    def _auth_token(self):
        return self._connection.auth_token

    @property
    def _db_opened(self):
        return self._connection.db_opened

    @property
    def _connected(self):
        return self._connection.connected

    @property
    def ready(self):
        return self._ready.is_set()

    def get_serializer(self):
        """
        Lazy return of the serialization, we retrive the type from the :class: `OrientSocket
        <aio_pyorient.orient.OrientSocket>` object
        :return: an Instance of the serializer suitable for decoding or encoding
        """
        if self._connection.serialization_type == OrientSerialization.Binary:
            return OrientSerialization.get_impl(self._connection.serialization_type,
                                                self._connection._props)
        else:
            return OrientSerialization.get_impl(self._connection.serialization_type)

    def get_orient_socket_instance(self):
        return self._connection

    def is_connected(self):
        """Deprecated! Use property connected instead"""
        return self._connected is True

    @property
    def connected(self):
        return self._connection.connected

    def database_opened(self):
        """Deprecated! Use property db_opened instead"""
        return self._db_opened

    @property
    def db_opened(self):
        return self._db_opened

    def get_cluster_map(self):
        """:type : list of [OrientNode]"""
        return self._node_list


    def set_session_token( self, token='' ):
        """
        :param token: Set the request to True to use the token authentication
        :type token: bool|string
        :return: self
        """
        if token != '' and token is not None:
            if isinstance(token, bool):
                self._request_token = token
            elif isinstance(token, (str, bytes)):
                self._request_token = True
                self._auth_token = token
                self._db_opened = True
                self._update_socket_token()
        return self

    def get_session_token( self ):
        """
        Retrieve the session token to reuse after
        :return:
        """
        return self._auth_token

    def _update_socket_id(self):
        """Force update of socket id from inside the class"""
        self._connection.session_id = self._session_id
        return self

    def _update_socket_token(self):
        """Force update of socket token from inside the class"""
        self._connection.auth_token = self._auth_token
        return self

    def _reset_fields_definition(self):
        self._fields_definition = []

    def prepare(self, *args):

        # session_id
        self._fields_definition.insert( 1, ( FIELD_INT, self._session_id ) )

        """
        #  Token authentication handling
        #  we must recognize ConnectMessage and DbOpenMessage messages
        """
        if self._need_token and self._request_token is True:
            self._fields_definition.insert(
                2, ( FIELD_STRING, self._auth_token )
            )

        self._output_buffer = b''.join(
            self._encode_field( x ) for x in self._fields_definition
        )
        return self

    @property
    def protocol(self):
        """OrientDb `Binary Protocol` version number, e.g. 37."""
        return self._connection.protocol

    async def _decode_header(self):
        self._header = [await self._decode_field(FIELD_BYTE),
                        await self._decode_field(FIELD_INT)]
        if self._header[0] == 1:

            # Parse the error
            exception_class = b''
            exception_message = b''

            more = await self._decode_field(FIELD_BOOLEAN)

            while more:
                # read num bytes by the field definition
                exception_class += await self._decode_field(FIELD_STRING)
                exception_message += await self._decode_field(FIELD_STRING)
                more = await self._decode_field(FIELD_BOOLEAN)

                if self.protocol > 18:  # > 18 1.6-snapshot
                    # read serialized version of exception thrown on server side
                    # useful only for java clients
                    serialized_exception = await self._decode_field(FIELD_STRING)
                    # trash
                    del serialized_exception

            raise PyOrientCommandException(
                exception_class.decode( 'utf8' ),
                [ exception_message.decode( 'utf8' ) ]
            )

        elif self._header[0] == 3:
            # Push notification, Node cluster changed
            # TODO: UNTESTED CODE!!!
            # FIELD_BYTE (OChannelBinaryProtocol.PUSH_DATA);  # WRITE 3
            # FIELD_INT (Integer.MIN_VALUE);  # SESSION ID = 2^-31
            # 80: \x50 Request Push 1 byte: Push command id
            push_command_id = await self._decode_field(FIELD_BYTE)
            push_message = await self._decode_field(FIELD_STRING)
            _, payload = self.get_serializer().decode(push_message)
            if self._push_callback:
                self._push_callback(push_command_id, payload)

            end_flag = await self._decode_field(FIELD_BYTE)

            # this flag can be set more than once
            while end_flag == 3:
                await self._decode_field(FIELD_INT)  # FAKE SESSION ID = 2^-31
                op_code = await self._decode_field(FIELD_BYTE)  # 80: 0x50 Request Push

                # REQUEST_PUSH_RECORD	        79
                # REQUEST_PUSH_DISTRIB_CONFIG	80
                # REQUEST_PUSH_LIVE_QUERY	    81
                if op_code == 80:
                    # for node in
                    payload = self.get_serializer().decode(
                        await self._decode_field(FIELD_STRING)
                    )  # JSON WITH THE NEW CLUSTER CFG

                    # reset the nodelist
                    self._node_list = []
                    for node in payload['members']:
                        self._node_list.append( OrientNode( node ) )

                end_flag = await self._decode_field(FIELD_BYTE)

            # Try to set the new session id???
            self._header[1] = await self._decode_field(FIELD_INT)  # REAL SESSION ID
            pass

        update_token = self._name in "ConnectMessage, DbOpenMessage" and self._request_token is True
        print("update token: ", update_token)
        if update_token:
            token_refresh = await self._decode_field(FIELD_STRING)
            print(f"token_refresh: {token_refresh}")
            if token_refresh != b'':
                self._auth_token = token_refresh
                self._update_socket_token()

    async def _decode_body(self):
        # read body
        for field in self._fields_definition:
            self._body.append(await self._decode_field(field))

        # clear field stack
        self._reset_fields_definition()
        return self

    async def _decode_all(self):
        await self._decode_header()
        await self._decode_body()

    async def fetch_response(self, *_continue):
        """
        # Decode header and body
        # If flag continue is set( Header already read ) read only body
        :param _continue:
        :return:
        """
        if len(_continue) is not 0:
            self._body = []
            await self._decode_body()
            # self.dump_streams()
        # already fetched, get last results as cache info
        elif len(self._body) is 0:
            await self._decode_all()
            # self.dump_streams()
        return self._body

    def _append(self, field):
        """
        @:rtype self: BaseMessage
        @type field: object
        """
        self._fields_definition.append( field )
        return self

    def __str__(self):

        return f"<{self._name} ready={self.ready}"

    async def send(self):
        if self._connection.in_transaction is False:
            await self._connection.write(self._output_buffer)
            self._reset_fields_definition()
        return self

    def close(self):
        self._connection.close()

    @staticmethod
    def _encode_field(field):

        # tuple with type
        t, v = field
        _content = None
        print(t, v)
        if t['type'] == INT:
            _content = struct.pack("!i", v)
        elif t['type'] == SHORT:
            _content = struct.pack("!h", v)
        elif t['type'] == LONG:
            _content = struct.pack("!q", v)
        elif t['type'] == BOOLEAN:
            if sys.version_info[0] < 3:
                _content = chr(1) if v else chr(0)
            else:
                _content = bytes([1]) if v else bytes([0])
        elif t['type'] == BYTE:
            if sys.version_info[0] < 3:
                _content = v
            else:
                _content = bytes([ord(v)])
        elif t['type'] == BYTES:
            _content = struct.pack("!i", len(v)) + v
        elif t['type'] == STRING:
            if sys.version_info[0] >= 3:
                if isinstance(v, str):
                    v = v.encode('utf-8')
            else:
                if isinstance(v, unicode):
                    v = v.encode('utf-8')
            _content = struct.pack("!i", len(v)) + v
        elif t['type'] == STRINGS:
            _content = b''
            for s in v:
                if sys.version_info[0] >= 3:
                    if isinstance(s, str):
                        s = s.encode('utf-8')
                else:
                    if isinstance(s, unicode):
                        s = s.encode('utf-8')
                _content += struct.pack("!i", len(s)) + s
        print(f"{t['type']} encoded: {_content}")
        return _content

    async def _decode_field(self, _type):
        _value = b""
        # read buffer length and decode value by field definition
        if _type['bytes'] is not None:
            _value = await self._connection.read(_type['bytes'])
        print(f"OrientSocket read type: {_type}")
        print(f"OrientSocket read type: {_value}")
        # if it is a string decode first 4 Bytes as INT
        # and try to read the buffer
        if _type['type'] == STRING or _type['type'] == BYTES:

            _len = struct.unpack('!i', _value)[0]
            if _len == -1 or _len == 0:
                _decoded_string = b''
            else:
                _decoded_string = await self._connection.read(_len)

            self._input_buffer += _value
            self._input_buffer += _decoded_string

            return _decoded_string

        elif _type['type'] == RECORD:

            # record_type
            record_type = await self._decode_field(_type['struct'][0])

            rid = "#" + str(await self._decode_field(_type['struct'][1]))
            rid += ":" + str(await self._decode_field(_type['struct'][2]))

            version = await self._decode_field(_type['struct'][3])
            content = await self._decode_field(_type['struct'][4])
            return {'rid': rid, 'record_type': record_type,
                    'content': content, 'version': version}

        elif _type['type'] == LINK:

            rid = "#" + str(await self._decode_field(_type['struct'][0]))
            rid += ":" + str(await self._decode_field(_type['struct'][1]))
            return rid

        else:
            self._input_buffer += _value

            if _type['type'] == BOOLEAN:
                return ord(_value) == 1
            elif _type['type'] == BYTE:
                return ord(_value)
            elif _type['type'] == CHAR:
                return _value
            elif _type['type'] == SHORT:
                return struct.unpack('!h', _value)[0]
            elif _type['type'] == INT:
                return struct.unpack('!i', _value)[0]
            elif _type['type'] == LONG:
                return struct.unpack('!q', _value)[0]

    async def _read_async_records(self):
        """
        # async-result-type byte as trailing byte of a record can be:
        # 0: no records remain to be fetched
        # 1: a record is returned as a result set
        # 2: a record is returned as pre-fetched to be loaded in client's
        #       cache only. It's not part of the result set but the client
        #       knows that it's available for later access
        """
        _status = await self._decode_field(FIELD_BYTE)  # status

        while _status != 0:

            try:

                # if a callback for the cache is not set, raise exception
                if not hasattr(self._callback, '__call__'):
                    raise AttributeError()

                _record = await self._read_record()

                if _status == 1:  # async record type
                    # async_records.append( _record )  # save in async
                    self._callback( _record )  # save in async
                elif _status == 2:  # cache
                    # cached_records.append( _record )  # save in cache
                    self._callback( _record )  # save in cache

            except AttributeError:
                # AttributeError: 'RecordLoadMessage' object has
                # no attribute '_command_type'
                raise PyOrientBadMethodCallException(
                    str(self._callback) + " is not a callable function", [])
            finally:
                # read new status and flush the debug buffer
                _status = self._decode_field( FIELD_BYTE )  # status

    async def _read_record(self):
        """
        # The format depends if a RID is passed or an entire
            record with its content.

        # In case of null record then -2 as short is passed.

        # In case of RID -3 is passes as short and then the RID:
            (-3:short)(cluster-id:short)(cluster-position:long).

        # In case of record:
            (0:short)(record-type:byte)(cluster-id:short)
            (cluster-position:long)(record-version:int)(record-content:bytes)

        :raise: PyOrientNullRecordException
        :return: OrientRecordLink,OrientRecord
        """
        marker = await self._decode_field(FIELD_SHORT)  # marker

        if marker is -2:
            raise PyOrientNullRecordException('NULL Record', [])
        elif marker is -3:
            res = OrientRecordLink(await self._decode_field(FIELD_TYPE_LINK))
        else:
            # read record
            __res = await self._decode_field(FIELD_RECORD)

            if self._connection.serialization_type == OrientSerialization.Binary:
                class_name, data = self.get_serializer().decode(__res['content'])
            else:
                # bug in orientdb csv serialization in snapshot 2.0
                class_name, data = self.get_serializer().decode(__res['content'].rstrip())


            res = OrientRecord(
                dict(
                    __o_storage=data,
                    __o_class=class_name,
                    __version=__res['version'],
                    __rid=__res['rid']
                )
            )

        # self.dump_streams()  # debug log
        self._output_buffer = b''
        self._input_buffer = b''

        return res
