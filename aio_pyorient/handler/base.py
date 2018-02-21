from collections import namedtuple
from pprint import pprint

from aio_pyorient.constants import NAME, SUPPORTED_PROTOCOL, VERSION
from aio_pyorient.handler.fields import Boolean, Byte, Bytes, Integer, Short, String
from aio_pyorient.serializations import OrientSerialization

ODBHeader = namedtuple('ODBHeader', 'status, session_id, auth_token')

HANDSHAKE = String.encode(NAME) + String.encode(VERSION) + Short.encode(SUPPORTED_PROTOCOL)


def get_request_header(op: int, session_id: int, auth_token: str):
    header = Byte.encode(chr(op)) + Integer.encode(session_id)
    if len(auth_token):
        print("encoding auth token: ", auth_token)
        header += String.encode(auth_token)
    print("request header: ", header)
    return header


odbHeader = namedtuple('ODBResponseHeade', 'status, session_id')


async def read_header(sock):
    print("read_header sock", sock)
    header = odbHeader(
        await Byte.decode(sock), await Integer.decode(sock)
    )
    print("read header:", header)
    return header


async def read_clusters(client):
    sock = client._sock
    cluster_count = await Short.decode(sock)
    print("cluster_count", cluster_count)
    for i in range(cluster_count):
        name = await String.decode(sock)
        c_id = await Short.decode(sock)
        client.clusters.append(cluster(name, c_id))
        print("cluster: ", name, c_id)
    return client.clusters


def get_connect_request(
        user: str, password: str, *,
        client_id: str = '',
        serialization_type: OrientSerialization = OrientSerialization.CSV,
        use_token_auth: bool = True,
        support_push: bool = True,
        collect_stats: bool = True):
    request = get_request_header(2, -1, '')
    request += HANDSHAKE
    request += String.encode(client_id)
    request += String.encode(serialization_type)
    request += Boolean.encode(use_token_auth)
    request += Boolean.encode(support_push)
    request += Boolean.encode(collect_stats)
    request += String.encode(user)
    request += String.encode(password)
    return request


async def connect_response(client):
    status, client._session_id = await read_header(client._sock)
    print(f"session_id: {client._session_id}")
    print(f"auth_token: {client._auth_token}")
    client._session_id, client._auth_token = [
        await Integer.decode(client._sock), await Bytes.decode(client._sock)
    ]
    print(f"session_id after: {client._session_id}")
    print(f"auth_token after: {client._auth_token}")
    return status


def get_db_open_request(
        db_name: str, user: str, password: str, *,
        client_id: str = '',
        serialization_type: OrientSerialization = OrientSerialization.CSV,
        use_token_auth: bool = True,
        support_push: bool = True,
        collect_stats: bool = True):
    request = get_request_header(3, -1, '')
    request += HANDSHAKE
    print("db_open_request header + handshake")
    print(request)
    request += String.encode(client_id)
    request += String.encode(serialization_type)
    request += Boolean.encode(use_token_auth)
    request += Boolean.encode(support_push)
    request += Boolean.encode(collect_stats)
    request += String.encode(db_name)
    request += String.encode(user)
    request += String.encode(password)
    print("db_open_request")
    print(request)
    return request


cluster = namedtuple('Cluster', 'name, c_id')


async def db_open_response(client):
    await connect_response(client)
    await read_clusters(client)
    client._cluster_conf = await Bytes.decode(client._sock)
    client._server_release = await String.decode(client._sock)
    pprint(vars(client))
    return client


async def db_reload_response(client):
    await connect_response(client)
    await read_clusters(client)
    return client
