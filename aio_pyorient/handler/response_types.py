from collections import namedtuple as nt

ErrorResponse = nt("ErrorResponse", "errors")

BaseResponse = nt("Base", "session_id, auth_token")
base = BaseResponse._fields

# Server
ServerConnectResponse = nt("ServerConnectResponse", base)

# Database
OpenDbResponse = nt(
    "OpenDbResponse",
    base + ("clusters", "cluster_conf", "server_version")
    )
ReloadDbResponse = nt("ReloadDbResponse", base + ("clusters",))

