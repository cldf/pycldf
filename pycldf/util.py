# coding: utf8
from __future__ import unicode_literals, print_function, division
from zipfile import ZipFile, ZIP_DEFLATED
from io import TextIOWrapper

from six import binary_type
from clldutils.path import Path, as_posix

CLDF_VERSION = 'cldf-1.0'
TABLE_TYPES = {
    'values': 'cldf-values',
}
MD_SUFFIX = '-metadata.json'


class Archive(ZipFile):
    def __init__(self, fname, mode='r'):
        ZipFile.__init__(
            self, as_posix(fname), mode=mode, compression=ZIP_DEFLATED, allowZip64=True)

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
