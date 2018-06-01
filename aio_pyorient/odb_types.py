import io
import re
from collections import namedtuple
from pprint import pprint

from aio_pyorient.serializer import Boolean, Float, Integer, IntegerList, List, String, StringList, Type

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

    def __repr__(self):
        return f"<ODBRecordData size {self.size} {self.decode()[:8]}...>"

class ODBRecord:
    def __init__(self, type, id, version, data):
        self.type = type
        self.id = id
        self.version = version
        self.data = ODBRecordData(data)

    def __repr__(self):
        return f'<ODBRecord id={self.id} version={self.version} {self.data.size}>'


class ODBClusters(list):

    def get(self, val: int or str)->list:
        is_id = isinstance(val, int)
        try:
            if is_id:
                return sorted([cl.name for cl in self if cl.id == val])
            return sorted([cl.id for cl in self if val.lower() in cl.name])
        except IndexError:
            raise ValueError(
                f"no cluster with {'id' if is_id else 'name'} {val}"
            )

CLASSES_SPECS = {
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
    'superClasses': StringList
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
strike_reg = re.compile(r'[_|-]')
def camel_case(word: str)->str:
    parts = strike_reg.split(word)
    return parts[0].lower() + ''.join([p.title() for p in parts[1:]])

class CSVSerializer:
    _re = {}
    @classmethod
    def __init_subclass__(cls, **kwargs):
        cls._keys = cls.__annotations__.keys()
        for k in cls._keys:
            cls._re[k] = re.compile(f",?{camel_case(k)}:").search

glob_props_re = re.compile(r'\(name:"(?P<name>\w+)",type:"(?P<type>\w+)",id:(?P<id>\d+)\)')
class ODBSchema(CSVSerializer):
    schema_version: int = 0
    classes: dict = {}
    global_properties: dict = {}
    blob_clusters: dict = {}

    def __init__(self, client):
        self.client = client

    async def get(self):
        raw = list(await self.client.execute('select from #0:1'))[0].data.decode()
        print('ODBSchema result')
        print(raw)
        matches = sorted([(*self._re[key](raw).span(), key) for key in self._keys])
        k_v = {}
        for i in range(len(matches)-1):
            k_v[matches[i][2]] = raw[matches[i][1]:matches[i+1][0]]
        k_v[matches[-1][2]] = raw[matches[-1][1]:]
        for m in glob_props_re.finditer(k_v['global_properties']):
            self.global_properties[int(m.group('id'))] = (m.group('name'), m.group('type'))
        pprint(self.global_properties)
        pprint(k_v['classes'][2:-2].split('),('))
        return {}

    def __str__(self):
        return f'<ODBSchema {sorted(list(self.classes.keys()))}>'

