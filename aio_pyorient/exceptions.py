"""
exceptions
"""
from collections.__init__ import namedtuple


ODBRequestErrorMessage = namedtuple("ODBException", "class_name, message")
