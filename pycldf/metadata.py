# coding: utf8
from __future__ import unicode_literals, print_function, division
from collections import OrderedDict

from clldutils import jsonlib

from pycldf.util import OptionalFile, CLDF_VERSION, TABLE_TYPES


class Metadata(dict, OptionalFile):
    def __init__(self, *args, **kw):
        dict.__init__(self, *args, **kw)
        for k, v in {
            "@context": ["http://www.w3.org/ns/csvw", {"@language": "en"}],
            "dc:format": CLDF_VERSION,
            "dialect": {"delimiter": ",", "encoding": "utf-8", "header": True},
            "tables": []
        }.items():
            self.setdefault(k, v)

    @property
    def values_table(self):
        return self.get_table('values')

    @property
    def dialect(self):
        return self['dialect']

    def get_table(self, type_):
        type_ = TABLE_TYPES[type_]
        for t in self['tables']:
            if t.get('dc:type') == type_:
                return t
        if type_ == TABLE_TYPES['values'] and len(self['tables']) == 1:
            return self['tables'][0]

    def get_column(self, type_, field, default=None):
        table = self.get_table(type_)
        if table:
            for col in table['tableSchema']['columns']:
                if col['name'] == field:
                    return col
        return default

    def add_table(self, type_, url, columns):
        assert self.get_table(type_) is None
        self['tables'].append({
            "url": url,
            "dc:type": TABLE_TYPES[type_],
            "tableSchema": {"columns": columns},
        })
        return self.get_table(type_)

    def read(self, fname):
        self.update(jsonlib.load(fname))

    def write(self, fname, **kw):
        out = OrderedDict()
        for key in sorted(
                self.keys(), key=lambda k: (not k.startswith('@'), ':' not in k, k)):
            out[key] = self[key]
        jsonlib.dump(out, fname, indent=4)
