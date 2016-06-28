# coding: utf8
from __future__ import unicode_literals, print_function, division

from clldutils.dsv import UnicodeReader, UnicodeWriter
from clldutils.path import Path

from pycldf.metadata import Table
from pycldf.util import Row, TAB_SUFFIXES, Archive


def _table_and_delimiter(table):
    if not isinstance(table, Table):
        table = Table(table)
    delimiter = table.dialect.get('delimiter', ',')
    if Path(table.url).suffix in TAB_SUFFIXES:
        delimiter = '\t'
    return table, delimiter


class Reader(UnicodeReader):
    def __init__(self, table, container=None, **kw):
        self.table, kw['delimiter'] = _table_and_delimiter(table)
        self.header_read = not self.table.dialect.header

        if isinstance(container, Archive):
            f = container.read_text(self.table.url).split('\n')
        elif isinstance(container, Path):
            f = container.joinpath(self.table.url)
        else:
            f = self.table.url  # pragma: no cover

        UnicodeReader.__init__(self, f, **kw)

    def __next__(self):
        row = UnicodeReader.__next__(self)
        if not self.header_read:
            assert row == [col.name for col in self.table.schema.columns.values()]
            row = UnicodeReader.__next__(self)
            self.header_read = True
        if not row:
            raise StopIteration
        return Row.from_list(self.table.schema, row)


class Writer(UnicodeWriter):
    def __init__(self, table, container=None, **kw):
        self.table, kw['delimiter'] = _table_and_delimiter(table)

        if isinstance(container, Archive):
            f = None
        elif isinstance(container, Path):
            f = container.joinpath(self.table.url)
        else:
            f = self.table.url  # pragma: no cover

        self.container = container
        self.header_written = not self.table.dialect.header
        UnicodeWriter.__init__(self, f, **kw)

    def writerow(self, row):
        if not self.header_written:
            UnicodeWriter.writerow(
                self, [col.name for col in self.table.schema.columns.values()])
            self.header_written = True
        if not isinstance(row, Row):
            row = Row.from_list(self.table.schema, row)
        else:
            assert row.schema == self.table.schema
        UnicodeWriter.writerow(self, row.to_list())

    def __exit__(self, type, value, traceback):
        UnicodeWriter.__exit__(self, type, value, traceback)
        if isinstance(self.container, Archive):
            self.container.write_text(self.read(), self.table.url)
