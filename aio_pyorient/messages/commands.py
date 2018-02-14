# -*- coding: utf-8 -*-
from datetime import datetime

from aio_pyorient.constants import (COMMAND_OP, FIELD_BOOLEAN, FIELD_BYTE, FIELD_CHAR, FIELD_INT, FIELD_LONG,
                                    FIELD_SHORT, FIELD_STRING, QUERY_ASYNC, QUERY_CMD, QUERY_GREMLIN, QUERY_SCRIPT,
                                    QUERY_SYNC, QUERY_TYPES, TX_COMMIT_OP)
from aio_pyorient.exceptions import PyOrientBadMethodCallException
from aio_pyorient.messages.base import BaseMessage
from aio_pyorient.messages.records import (RecordCreateMessage, RecordDeleteMessage, RecordUpdateMessage)


class CommandMessage(BaseMessage):

    def __init__(self, _orient_socket):
        super().__init__(_orient_socket)
        self._query = ''
        self._limit = 20
        self._fetch_plan = '*:0'
        self._command_type = QUERY_SYNC
        self._mod_byte = 's'

        self._append( ( FIELD_BYTE, COMMAND_OP ) )

    def prepare(self, params=None ):

        if isinstance(params, (tuple, list)):
            command_type = params[0]
            try:
                if command_type in QUERY_TYPES:
                    # user choice if present
                    self._command_type = command_type
                else:
                    raise PyOrientBadMethodCallException(
                        command_type + ' is not a valid command type', []
                    )

                self._query = params[1]
                self._limit = params[2]
                self._fetch_plan = params[3]

                # callback function use to operate
                # over the async fetched records
                self.set_callback( params[4] )

            except IndexError:
                # Use default for non existent indexes
                pass

        if self._command_type == QUERY_CMD \
                or self._command_type == QUERY_SYNC \
                or self._command_type == QUERY_SCRIPT \
                or self._command_type == QUERY_GREMLIN:
            self._mod_byte = 's'
        else:
            if self._callback is None:
                raise PyOrientBadMethodCallException( "No callback was provided.", [])
            self._mod_byte = 'a'

        _payload_definition = [
            ( FIELD_STRING, self._command_type ),
            ( FIELD_STRING, self._query )
        ]

        if self._command_type == QUERY_ASYNC \
                or self._command_type == QUERY_SYNC \
                or self._command_type == QUERY_GREMLIN:

            # a limit specified in a sql string should always override a
            # limit parameter pass to prepare()
            if ' LIMIT ' not in self._query.upper() or self._command_type == QUERY_GREMLIN:
                _payload_definition.append( ( FIELD_INT, self._limit ) )
            else:
                _payload_definition.append( ( FIELD_INT, -1 ) )

            _payload_definition.append( ( FIELD_STRING, self._fetch_plan ) )

        if self._command_type == QUERY_SCRIPT:
            _payload_definition.insert( 1, ( FIELD_STRING, 'sql' ) )

        _payload_definition.append( ( FIELD_INT, 0 ) )

        payload = b''.join(
            self._encode_field( x ) for x in _payload_definition
        )

        self._append( ( FIELD_BYTE, self._mod_byte ) )
        self._append( ( FIELD_STRING, payload ) )

        return super().prepare()

    async def fetch_response(self):

        # skip execution in case of transaction
        if self._connection.in_transaction is True:
            return self

        # decode header only
        await super().fetch_response()

        if self._command_type == QUERY_ASYNC:
            await self._read_async_records()
        else:
            return await self._read_sync()

    async def _read_sync(self):

        response_type = await self._decode_field(FIELD_CHAR)
        if not isinstance(response_type, str):
            response_type = response_type.decode()
        res = []
        if response_type == 'n':
            self._append( FIELD_CHAR )
            await super().fetch_response(True)
            return None
        elif response_type in 'rw':
            res = [await self._read_record()]
            self._append( FIELD_CHAR )
            await super().fetch_response(True)
            if response_type == 'w':
                res = [ res[0].oRecordData['result'] ]
        elif response_type == 'a':
            self._append( FIELD_STRING )
            self._append( FIELD_CHAR )
            res = [await super().fetch_response(True)[0]]
        elif response_type == 'l':
            self._append( FIELD_INT )
            list_len = await super().fetch_response(True)[0]

            for n in range(0, list_len):
                res.append(await self._read_record())

            # async-result-type can be:
            # 0: no records remain to be fetched
            # 1: a record is returned as a result set
            # 2: a record is returned as pre-fetched to be loaded in client's
            #       cache only. It's not part of the result set but the client
            #       knows that it's available for later access
            cached_results = await self._read_async_records()
            # cache = cached_results['cached']
        else:
            # this should be never happen, used only to debug the protocol
            msg = b''
            # self._orientSocket._socket.setblocking( 0 )
            m = await self._connection.read(1)
            while m != "":
                msg += m
                m = await self._connection.read(1)

        return res

    def set_callback(self, func):
        if hasattr(func, '__call__'):
            self._callback = func
        else:
            raise PyOrientBadMethodCallException( func + " is not a callable "
                                                         "function", [])
        return self

class _TXCommitMessage(BaseMessage):
    def __init__(self, _orient_socket):
        super().__init__(_orient_socket)

        self._tx_id = -1
        self._operation_stack = []
        self._pre_operation_records = {}
        self._operation_records = {}

        self._temp_cluster_position_seq = -2

        # order matters
        self._append(( FIELD_BYTE, TX_COMMIT_OP ))
        self._command = TX_COMMIT_OP

    def prepare(self, params=None):

        self._append(( FIELD_INT, self.get_transaction_id() ))
        self._append(( FIELD_BOOLEAN, True ))

        for k, v in enumerate(self._operation_stack):
            self._append(( FIELD_BYTE, chr(1) ))  # start of records
            for field in v:
                self._append(field)

        self._append(( FIELD_BYTE, chr(0) ))
        self._append(( FIELD_STRING, "" ))

        return super().prepare()

    async def fetch_response(self):
        await super().fetch_response()

        result = {
            'created': [],
            'updated': [],
            'changes': []
        }

        items = await self._decode_field(FIELD_INT)
        for x in range(0, items):
            result['created'].append(
                {
                    'client_c_id': await self._decode_field(FIELD_SHORT),
                    'client_c_pos': await self._decode_field(FIELD_LONG),
                    'created_c_id': await self._decode_field(FIELD_SHORT),
                    'created_c_pos': await self._decode_field(FIELD_LONG)
                }
            )

            operation = self._pre_operation_records[
                str(result['created'][-1]['client_c_pos'])
            ]

            rid = "#" + str(result['created'][-1]['created_c_id']) + \
                  ":" + str(result['created'][-1]['created_c_pos'])

            record = getattr(operation, "_record_content")
            record.update(__version=1, __rid=rid)

            self._operation_records[rid] = record

        items = await self._decode_field(FIELD_INT)
        for x in range(0, items):
            result['updated'].append(
                {
                    'updated_c_id': await self._decode_field(FIELD_SHORT),
                    'updated_c_pos': await self._decode_field(FIELD_LONG),
                    'new_version': await self._decode_field(FIELD_INT),
                }
            )

            try:
                operation = self._pre_operation_records[
                    str(result['updated'][-1]['updated_c_pos'])
                ]
                record = getattr(operation, "_record_content")
                rid = "#" + str(result['updated'][-1]['updated_c_id']) + \
                      ":" + str(result['updated'][-1]['updated_c_pos'])
                record.update(
                    __version=result['updated'][-1]['new_version'],
                    __rid=rid
                )

                self._operation_records[rid] = record

            except KeyError:
                pass

        if self.protocol > 23:
            items = await self._decode_field(FIELD_INT)
            for x in range(0, items):
                result['updated'].append(
                    {
                        'uuid_high': await self._decode_field(FIELD_LONG),
                        'uuid_low': await self._decode_field(FIELD_LONG),
                        'file_id': await self._decode_field(FIELD_LONG),
                        'page_index': await self._decode_field(FIELD_LONG),
                        'page_offset': await self._decode_field(FIELD_INT),
                    }
                )

        return self._operation_records

    async def attach(self, operation):

        if not isinstance(operation, BaseMessage):
            # A Subclass of BaseMessage was expected
            raise AssertionError("A subclass of BaseMessage was expected")

        if isinstance(operation, RecordUpdateMessage):
            o_record_enc = self.get_serializer().encode(getattr(operation, "_record_content"))
            self._operation_stack.append((
                ( FIELD_BYTE, chr(1) ),
                ( FIELD_SHORT, int(getattr(operation, "_cluster_id")) ),
                ( FIELD_LONG, int(getattr(operation, "_cluster_position")) ),
                ( FIELD_BYTE, getattr(operation, "_record_type") ),
                ( FIELD_INT, int(getattr(operation, "_record_version")) ),
                ( FIELD_STRING, o_record_enc ),
            ))

            if self.protocol >= 23:
                self._operation_stack[-1] = \
                    self._operation_stack[-1] +\
                    ( ( FIELD_BOOLEAN, bool(getattr(operation, "_update_content") ) ), )

            self._pre_operation_records[
                str(getattr(operation, "_cluster_position"))
            ] = operation

        elif isinstance(operation, RecordDeleteMessage):
            self._operation_stack.append((
                ( FIELD_BYTE, chr(2) ),
                ( FIELD_SHORT, int(getattr(operation, "_cluster_id")) ),
                ( FIELD_LONG, int(getattr(operation, "_cluster_position")) ),
                ( FIELD_BYTE, getattr(operation, "_record_type") ),
                ( FIELD_INT, int(getattr(operation, "_record_version")) ),
            ))
        elif isinstance(operation, RecordCreateMessage):
            o_record_enc = self.get_serializer().encode(getattr(operation, "_record_content"))
            self._operation_stack.append((
                ( FIELD_BYTE, chr(3) ),
                ( FIELD_SHORT, int(-1) ),
                ( FIELD_LONG, int(self._temp_cluster_position_seq) ),
                ( FIELD_BYTE, getattr(operation, "_record_type") ),
                ( FIELD_STRING, o_record_enc ),
            ))
            self._pre_operation_records[
                str(self._temp_cluster_position_seq)
            ] = operation
            self._temp_cluster_position_seq -= 1
        else:
            raise PyOrientBadMethodCallException(
                "Wrong command type " + operation.__class__.__name__, []
            )

        return self

    async def get_transaction_id(self):

        if self._tx_id < 0:

            my_epoch = datetime(2014, 7, 1)
            now = datetime.now()
            delta = now - my_epoch

            # write in extended mode to make it easy to read
            # seconds * 1000000 to get the equivalent microseconds
            _sm = ( delta.seconds + delta.days * 24 * 3600 ) * 10 ** 6
            _ms = delta.microseconds
            _mstime = _sm + _ms
            # remove sign
            # treat as unsigned even when the INT is signed
            # and take 4 Bytes
            #   ( 32 bit uniqueness is not ensured in any way,
            #     but is surely unique in this session )
            # we need only a transaction unique for this session
            # not a real UUID
            if _mstime & 0x80000000:
                self._tx_id = int(( _mstime - 0x80000000 ) & 0xFFFFFFFF)
            else:
                self._tx_id = int(_mstime & 0xFFFFFFFF)

        return self._tx_id

    async def begin(self):
        self._operation_stack = []
        self._pre_operation_records = {}
        self._operation_records = {}
        self._temp_cluster_position_seq = -2
        self._connection.in_transaction = True
        await self.get_transaction_id()
        return self

    async def commit(self):
        self._connection.in_transaction = False
        request = self.prepare(())
        await request.send()
        result = await request.fetch_response()
        self._operation_stack = []
        self._pre_operation_records = {}
        self._operation_records = {}
        self._tx_id = -1
        self._temp_cluster_position_seq = -2
        return result

    async def rollback(self):
        self._operation_stack = []
        self._pre_operation_records = {}
        self._operation_records = {}
        self._tx_id = -1
        self._temp_cluster_position_seq = -2
        self._connection.in_transaction = False
        return self

#
# TX COMMIT facade
#
class TxCommitMessage:

    def __init__(self, _orient_socket):
        self._transaction = _TXCommitMessage(_orient_socket)
        pass

    def attach(self, operation):
        self._transaction.attach( operation )
        return self

    def begin(self):
        self._transaction.begin()
        return self

    async def commit(self):
        return await self._transaction.commit()


