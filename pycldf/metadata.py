# coding: utf8
from __future__ import unicode_literals, print_function, division

from clldutils import jsonlib

from pycldf.util import OptionalFile


class Metadata(OptionalFile):
    def __init__(self):
        self._md = {
            "@context": ["http://www.w3.org/ns/csvw", {"@language": "en"}],
            "dc:format": "cldf-1.0",
            "dialect": {
                "delimiter": ",",
                "encoding": "utf-8",
                "header": True
            },
            "tableSchema": {}
        }

    def __getitem__(self, item):
        return self._md[item]

    def __setitem__(self, key, value):
        self._md[key] = value

    def __contains__(self, item):
        return item in self._md

    def get(self, item, default=None):
        if item in self:
            return self[item]
        return default

    def setdefault(self, item, value):
        if item not in self:
            self[item] = value

    def read(self, fname):
        self._md = jsonlib.load(fname)

    def write(self, fname):
        jsonlib.dump(self._md, fname, indent=4)
