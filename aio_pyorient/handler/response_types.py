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

DbExistResponse = nt("DbExistResponse", base + ("exist",))

DbSizeResponse = nt("DbSizeResponse", base + ("size",))

DbRecordCountResponse = nt("DbRecordCountResponse", base + ("count",))

CreateDbResponse = nt("CreateDbResponse", base)

DropDbResponse = nt("DropDbResponse", base)

CloseDbResponse = None  # socket got closed and so no response available

# Command
QueryCommandResponse = nt("QueryCommandResponse", base + ("records", "prefetched"))
