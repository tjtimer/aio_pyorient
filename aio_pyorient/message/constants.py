# constants
SERVER_CONNECT = 2
OPEN_DB = 3
CREATE_DB = 4
CLOSE_DB = 5
DB_EXIST = 6
DROP_DB = 7
DB_SIZE = 8
DB_RECORD_COUNT = 9
DB_COMMAND = 41
RELOAD_DB = 73

QUERY_SYNC    = "com.orientechnologies.orient.core.sql.query.OSQLSynchQuery"
QUERY_ASYNC   = "com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery"
QUERY_CMD     = "com.orientechnologies.orient.core.sql.OCommandSQL"
QUERY_GREMLIN = "com.orientechnologies.orient.graph.gremlin.OCommandGremlin"
QUERY_SCRIPT  = "com.orientechnologies.orient.core.command.script.OCommandScript"
NAME = "ODB binary client (aio_pyorient)"
VERSION = "0.0.1"
SUPPORTED_PROTOCOL = 37
REQUEST_SUCCESS = 0
REQUEST_ERROR = 1
REQUEST_PUSH = 3
