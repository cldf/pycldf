# coding: utf8
from __future__ import unicode_literals, print_function, division


class OptionalFile(object):
    def read_if_exists(self, fname):
        if fname.exists():
            self.read(fname)

    def read(self, fname):  # pragma: no cover
        raise NotImplemented

    def write(self, fname):  # pragma: no cover
        raise NotImplemented
