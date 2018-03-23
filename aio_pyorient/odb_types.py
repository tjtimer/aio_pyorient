from collections import namedtuple

from aio_pyorient.serializer import csv_serialize, Boolean, Integer, List, String, Float, Type, IntegerList, StringList


ODBRecord = namedtuple("ODBRecord", "type, id, version, data")
ODBCluster = namedtuple('ODBCluster', 'name, id')


class ODBClusters(list):

    def get(self, prop: int or str)->str or int:
        is_id = isinstance(prop, int)
        attr_type = 'id' if is_id else 'name'
        try:
            clusters = [cl for cl in self if prop in cl]
        except IndexError:
            raise ValueError(
                f"cluster with {attr_type} {prop} does not exist"
            )
        else:
            if is_id:
                return [cluster.name for cluster in clusters]
            return [cluster.id for cluster in clusters]

SCHEMA_SPECS = {
    'abstract': Boolean,
    'clusterIds': IntegerList,
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
    'superClasses': StringList,
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
                    csv_serialize(_p, PROPS_SPECS) for _p in p_string.split('),(')
                ]
                d_string = d_string[0:props_start] + d_string[props_end+1:]
            data = csv_serialize(d_string, SCHEMA_SPECS)
            data['properties'] = {prop['name']: prop for prop in props}
            self._classes[data['name']] = data

    def __str__(self):
        return f'<ODBSchema {sorted(list(self._classes.keys()))}>'

    @property
    def classes(self):
        return self._classes
