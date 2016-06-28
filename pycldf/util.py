# coding: utf8
from __future__ import unicode_literals, print_function, division
from zipfile import ZipFile, ZIP_DEFLATED
from io import TextIOWrapper
from collections import OrderedDict

from six import binary_type
from clldutils.path import Path, as_posix
from uritemplate import expand

__all__ = ['Archive']

CLDF_VERSION = 'cldf-1.0'
TABLE_TYPES = {
    'values': 'cldf-values',
}
MD_SUFFIX = '-metadata.json'
TAB_SUFFIXES = ['.tsv', '.tab']


class Row(OrderedDict):
    def __init__(self, schema):
        self.schema = schema
        OrderedDict.__init__(self)

    @classmethod
    def from_list(cls, schema, row):
        if not isinstance(row, (list, tuple)):
            raise TypeError(type(row))  # pragma: no cover
        d = cls(schema)
        if len(row) != len(d.schema.columns):
            raise ValueError('wrong number of columns in row')
        for col, value in zip(d.schema.columns.values(), row):
            d[col.name] = col.unmarshal(value)
        return d

    def to_list(self):
        return [
            col.marshal(v) for col, v in zip(self.schema.columns.values(), self.values())]

    @property
    def url(self):
        if self.schema.aboutUrl:
            return expand(self.schema.aboutUrl, self)

    def valueUrl(self, col):
        if self[col] is not None:
            if self.schema.columns[col].valueUrl:
                return expand(self.schema.columns[col].valueUrl, self)


class Archive(ZipFile):
    def __init__(self, fname, mode='r'):
        ZipFile.__init__(
            self, as_posix(fname), mode=mode, compression=ZIP_DEFLATED, allowZip64=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def metadata_name(self, prefix=None):
        for name in self.namelist():
            if name.endswith(MD_SUFFIX) and (prefix is None or name.startswith(prefix)):
                return name
        raise ValueError('no metadata file found')  # pragma: no cover

    def read_text(self, name):
        if name in self.namelist():
            return TextIOWrapper(self.open(name), encoding='utf8').read()

    def write_text(self, text, name):
        if not isinstance(text, binary_type):
            text = text.encode('utf8')
        self.writestr(name, text)


class OptionalData(object):
    def read(self, name, container):
        text = None
        if isinstance(container, Archive):
            text = container.read_text(name)
        elif isinstance(container, Path):
            fname = container.joinpath(name)
            if fname.exists():
                with fname.open(encoding='utf8') as fp:
                    text = fp.read()
        else:
            raise ValueError(type(container))  # pragma: no cover
        if text:
            self.read_string(text)

    def read_string(self, text):  # pragma: no cover
        raise NotImplemented

    def write(self, name, container, **kw):
        text = self.write_string(**kw)
        if text:
            if isinstance(container, Archive):
                container.write_text(text, name)
            elif isinstance(container, Path):
                fname = container.joinpath(name)
                with fname.open('w', encoding='utf8') as fp:
                    if isinstance(text, binary_type):
                        text = text.decode('utf8')
                    fp.write(text)
            else:
                raise ValueError(type(container))  # pragma: no cover

    def write_string(self, **kw):  # pragma: no cover
        return ''
