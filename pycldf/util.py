# coding: utf8
from __future__ import unicode_literals, print_function, division


CLDF_VERSION = 'cldf-1.0'
TABLE_TYPES = {
    'values': 'cldf-values',
}


class OptionalFile(object):
    @classmethod
    def from_file(cls, fname):
        res = cls()
        if fname.exists():
            res.read(fname)
        return res

    def read(self, fname):  # pragma: no cover
        raise NotImplemented

    def write(self, fname, **kw):  # pragma: no cover
        raise NotImplemented
