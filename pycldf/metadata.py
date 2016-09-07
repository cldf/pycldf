# coding: utf8
from __future__ import unicode_literals, print_function, division
from collections import OrderedDict, defaultdict, MutableMapping
import json
from datetime import date, datetime

from dateutil.parser import parse as datetime_parser

from pycldf.util import OptionalData, CLDF_VERSION, TABLE_TYPES

__all__ = []


def boolean(s):
    """
    .. seealso:: https://www.w3.org/TR/xmlschema11-2/#f-booleanLexmap
    """
    if isinstance(s, bool):
        return s
    if s.lower() in ['true', 'true', '1', 'yes', 'y']:
        return True
    if s.lower() in ['false', '0', 'no', 'n']:
        return False
    raise ValueError('invalid lexical value for boolean: %s' % s)


def identity(x):
    return x


def string(x):
    return '%s' % x


def parse_date(x):
    if isinstance(x, datetime):
        x = x.date()
    if isinstance(x, date):
        return x
    return datetime_parser(x).date()


def parse_datetime(x):
    if isinstance(x, datetime):
        return x
    return datetime_parser(x)


def parse_json(x):
    if isinstance(x, (dict, list)):
        return x
    return json.loads(x)


TYPE_MAP = defaultdict(
    lambda: (identity, string),
    integer=(int, string),
    decimal=(float, string),
    float=(float, string),
    boolean=(boolean, string),
    datetime=(parse_datetime, lambda o: o.isoformat()),
    date=(parse_date, lambda o: o.isoformat()),
    json=(parse_json, json.dumps),
)


class DictWrapper(MutableMapping):
    def __init__(self, d):
        self._d = d

    def __len__(self):
        return len(self._d)

    def __delitem__(self, key):
        del self._d[key]

    def __contains__(self, item):
        return item in self._d

    def __iter__(self):
        return iter(self._d)

    def __getitem__(self, item):
        return self._d[item]

    def __setitem__(self, key, value):
        self._d[key] = value

    def get(self, item, default=None):
        return self._d.get(item, default)

    def keys(self):
        return self._d.keys()

    def items(self):
        return self._d.items()

    def values(self):
        return self._d.values()


class Dialect(DictWrapper):
    @property
    def header(self):
        if 'header' not in self:
            self['header'] = True
        return self['header']

    @header.setter
    def header(self, value):
        self['header'] = value

    @property
    def delimiter(self):
        if 'delimiter' not in self:
            self['delimiter'] = ','
        return self['delimiter']

    @delimiter.setter
    def delimiter(self, value):
        self['delimiter'] = value


class Column(DictWrapper):
    @property
    def datatype(self):
        return self['datatype']

    @datatype.setter
    def datatype(self, value):  # pragma: no cover
        if value is int:
            value = 'integer'
        elif value is float:
            value = 'float'
        elif value is bool:
            value = 'boolean'
        elif value is date:
            value = 'date'
        elif value is datetime:
            value = 'datetime'
        elif value is dict or value is list or value is tuple:
            value = 'json'
        self['datatype'] = value

    @property
    def name(self):
        return self['name']

    @property
    def valueUrl(self):
        return self.get('valueUrl')

    @valueUrl.setter
    def valueUrl(self, value):
        self['valueUrl'] = value

    def marshal(self, value):
        if value is None:
            return ''
        return TYPE_MAP[self.datatype][1](value)

    def unmarshal(self, value):
        if value == '':
            return None
        return TYPE_MAP[self.datatype][0](value)


class Schema(DictWrapper):
    @property
    def columns(self):
        res = OrderedDict()
        for d in self['columns']:
            res[d['name']] = Column(d)
        return res

    @property
    def aboutUrl(self):
        return self.get('aboutUrl')

    @aboutUrl.setter
    def aboutUrl(self, value):
        self['aboutUrl'] = value

    @property
    def primaryKey(self):
        return self.get('primaryKey')

    @primaryKey.setter
    def primaryKey(self, value):
        if value not in self.columns:
            raise ValueError('primary key is not a column name: %s' % value)
        self['primaryKey'] = value


class Table(DictWrapper):
    def __init__(self, d, group=None):
        DictWrapper.__init__(self, d)
        self.group = group

    @property
    def schema(self):
        return Schema(self['tableSchema'])

    @property
    def url(self):
        return self.get('url')

    @url.setter
    def url(self, value):
        self['url'] = value

    @property
    def dialect(self):
        if self.get('dialect'):
            return Dialect(self['dialect'])
        if self.group and 'dialect' in self.group:
            return Dialect(self.group['dialect'])
        self['dialect'] = {"delimiter": ",", "encoding": "utf-8", "header": True}
        return Dialect(self['dialect'])


class Metadata(dict, OptionalData):
    def __init__(self, *args, **kw):
        dict.__init__(self, *args, **kw)
        for k, v in {
            "@context": ["http://www.w3.org/ns/csvw", {"@language": "en"}],
            "dc:format": CLDF_VERSION,
            "dialect": {"delimiter": ",", "encoding": "utf-8", "header": True},
            "tables": []
        }.items():
            self.setdefault(k, v)

    @classmethod
    def from_file(cls, fname):
        md = cls()
        md.read(fname.name, fname.parent)
        return md

    def get_table(self, type_='values'):
        type_ = TABLE_TYPES[type_]
        for t in self['tables']:
            if t.get('dc:type') == type_:
                return Table(t, self)
        if type_ == TABLE_TYPES['values'] and len(self['tables']) == 1:
            return Table(self['tables'][0], self)

    def add_table(self, type_, url, columns):
        assert self.get_table(type_) is None
        self['tables'].append({
            "url": url,
            "dc:type": TABLE_TYPES[type_],
            "tableSchema": {"columns": columns},
        })
        return self.get_table(type_)

    def read_string(self, text):
        self.update(json.loads(text))

    def write_string(self, **kw):
        out = OrderedDict()
        for key in sorted(
                self.keys(), key=lambda k: (not k.startswith('@'), ':' not in k, k)):
            out[key] = self[key]
        return json.dumps(out, indent=4)
