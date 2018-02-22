from collections import namedtuple

from aio_pyorient.constants import NAME, SUPPORTED_PROTOCOL, VERSION
from aio_pyorient.handler.fields import Boolean, Byte, Bytes, Integer, Short, String
from aio_pyorient.serializations import OrientSerialization

ODBHeader = namedtuple('ODBHeader', 'status, session_id, auth_token')

HANDSHAKE = String.encode(NAME) + String.encode(VERSION) + Short.encode(SUPPORTED_PROTOCOL)


def get_request_header(op: int, session_id: int, auth_token: str = None):
    header = Byte.encode(chr(op)) + Integer.encode(session_id)
    if auth_token:
        print("encoding auth token: ", auth_token)
        header += Bytes.encode(auth_token)
    print("request header: ", header)
    return header


ODBResponseHeader = namedtuple('ODBResponseHeader', 'status, session_id')
ODBCluster = namedtuple('ODBCluster', 'name, c_id')

async def read_header(sock):
    return ODBResponseHeader(
        await Byte.decode(sock), await Integer.decode(sock)
    )


async def read_clusters(client):
    for _ in range(await Short.decode(client._sock)):
        client.clusters.append(
            ODBCluster(await String.decode(client._sock), await Short.decode(client._sock))
        )
    return client

def get_connect_request(
        user: str, password: str, *,
        client_id: str = '',
        serialization_type: OrientSerialization = OrientSerialization.CSV,
        use_token_auth: bool = True,
        support_push: bool = True,
        collect_stats: bool = True):
    return b''.join(
        (
            get_request_header(2, -1),
            HANDSHAKE,
            String.encode(client_id),
            String.encode(serialization_type),
            Boolean.encode(use_token_auth),
            Boolean.encode(support_push),
            Boolean.encode(collect_stats),
            String.encode(user),
            String.encode(password)
        )
    )

def get_db_open_request(
        db_name: str, user: str, password: str, *,
        client_id: str = '',
        serialization_type: OrientSerialization = OrientSerialization.CSV,
        use_token_auth: bool = True,
        support_push: bool = True,
        collect_stats: bool = True):
    return b''.join(
        (
            get_request_header(3, -1),
            HANDSHAKE,
            String.encode(client_id),
            String.encode(serialization_type),
            Boolean.encode(use_token_auth),
            Boolean.encode(support_push),
            Boolean.encode(collect_stats),
            String.encode(db_name),
            String.encode(user),
            String.encode(password)
        )
    )


async def connect_response(client):
    await read_header(client._sock)
    client._session_id, client._auth_token = [
        await Integer.decode(client._sock), await Bytes.decode(client._sock)
    ]
    return client


async def db_open_response(client):
    await connect_response(client)
    await read_clusters(client)
    client._cluster_conf = await Bytes.decode(client._sock)
    client._server_release = await String.decode(client._sock)
    return client


async def db_reload_response(client):
    await connect_response(client)
    await read_clusters(client)
    return client
