import io
from collections import namedtuple

from aio_pyorient.serializer import serialize, Boolean, Integer, List, String, Float, Type

ODBCluster = namedtuple('ODBCluster', 'name, id')
ODBRequestErrorMessage = namedtuple("ODBException", "class_name, message")

class ODBRecordData(io.BytesIO):

    def __init__(self, initial: bytes=None):
        super().__init__(initial)

    @property
    def size(self):
        return self.__sizeof__()

    def decode(self):
        return self.getvalue().decode()

class ODBRecord:
    def __init__(self, type, id, version, data):
        self.type = type
        self.id = id
        self.version = version
        self.data = ODBRecordData(data)

    def __repr__(self):
        return f'<ODBRecord id={self.id} version={self.version} {self.data.size}>'


class ODBClusters(list):

    def get(self, prop: int or str)->str or int:
        is_id = isinstance(prop, int)
        attr_type = 'id' if is_id else 'name'
        try:
            cluster = [cl for cl in self if prop in cl][0]
        except IndexError:
            raise ValueError(
                f"cluster with {attr_type} {prop} does not exist"
            )
        else:
            if is_id:
                return cluster.name
            return cluster.id

SCHEMA_SPECS = {
    'abstract': Boolean,
    'clusterIds': List,
    'clusterSelection': String,
    'customFields': String,
    'defaultClusterId': Integer,
    'description': String,
    'name': String,
    'overSize': Float,
    'properties': String,
    'shortName': String,
    'strictMode': Boolean,
    'superClass': String,
    'superClasses': List,
    'type': Type,
    'notNull': Boolean,
    'mandatory': Boolean
}
PROPS_SPECS = {
    'collate': String,
    'customFields': List,
    'defaultValue': String,
    'description': String,
    'globalId': Integer,
    'mandatory': Boolean,
    'max': Integer,
    'min': Integer,
    'name': String,
    'notNull': Boolean,
    'readonly': Boolean,
    'type': Type
}

class ODBSchema:
    def __init__(self, records):
        self._classes = {}
        for record in records:
            d_string = record.data.decode()
            props = []
            props_start = d_string.find('<(')
            if props_start >= 0:
                props_end = d_string.find(')>')
                p_string = d_string[props_start + 1:props_end]
                props = [
                    serialize(_p, PROPS_SPECS) for _p in p_string.split('),(')
                ]
                d_string = d_string[0:props_start] + d_string[props_end+1:]
            data = serialize(d_string, SCHEMA_SPECS)
            data['properties'] = {prop['name']: prop for prop in props}
            self._classes[data['name']] = data

    def __str__(self):
        return f'<ODBSchema {sorted(list(self._classes.keys()))}>'

    @property
    def classes(self):
        return self._classes
