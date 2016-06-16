# coding: utf8
from __future__ import unicode_literals, print_function, division
from collections import OrderedDict

from clldutils.path import Path
from clldutils.dsv import reader, UnicodeWriter

from pycldf.sources import Sources
from pycldf.metadata import Metadata


TAB_SUFFIXES = ['.tsv', '.tab']
REQUIRED_FIELDS = [('ID',), ('Language_ID',), ('Parameter_ID', 'Feature_ID'), ('Value',)]


class Dataset(object):
    def __init__(self, name):
        self.name = name
        self.sources = Sources()
        self.metadata = Metadata()
        self._rows = OrderedDict()
        self._fields = ()
        self._source_count = None

    def __len__(self):
        return len(self.rows)

    def __getitem__(self, item):
        if isinstance(item, int):
            return self.rows[item]
        return self._rows[item]

    @property
    def fields(self):
        return self._fields

    @fields.setter
    def fields(self, value):
        if self._fields:
            raise ValueError('fields can only be assigned once!')
        assert isinstance(value, tuple)
        assert all(any(field in value for field in variants)
                   for variants in REQUIRED_FIELDS)
        if 'columns' in self.metadata.get('tableSchema', {}):
            assert list(value) == [
                col['name'] for col in self.metadata['tableSchema']['columns']]

        else:
            self.metadata.setdefault('tableSchema', {})
            self.metadata['tableSchema']['columns'] = [
                {'name': col, 'datatype': 'string'} for col in value]
            self.metadata['tableSchema']['primaryKey'] = 'ID'
        self._fields = value

    @property
    def rows(self):
        return list(self._rows.values())

    def add_row(self, row):
        assert len(row) == len(self.fields)
        d = OrderedDict()
        for k, v in zip(self.fields, row):
            d[k] = v
        assert d['ID'] not in self._rows
        self.sources.check(d)
        self._rows[d['ID']] = d

    def source_count(self):
        return self.sources.check(*self.rows)

    def bib_name(self):
        return self.name + '.bib'

    def metadata_name(self, suffix='.csv'):
        return self.name + suffix + '-metadata.json'

    #
    # FIXME: support reading and writing zip archives!
    #
    @classmethod
    def from_file(cls, fname):
        fname = Path(fname)
        assert fname.exists() and fname.is_file()
        dataset = cls(fname.stem)
        dataset.metadata.read_if_exists(
            fname.parent.joinpath(dataset.metadata_name(suffix=fname.suffix)))
        dataset.sources.read_if_exists(fname.parent.joinpath(dataset.bib_name()))
        if fname.suffix in TAB_SUFFIXES:
            dataset.metadata['dialect']['delimiter'] = '\t'
        for i, row in enumerate(reader(
            fname, delimiter=dataset.metadata.get('dialect', {}).get('delimiter', ',')
        )):
            if i == 0:
                dataset.fields = tuple(row)
            else:
                dataset.add_row(row)
        return dataset

    def write(self, outdir='.', suffix='.csv'):
        fname = Path(outdir).joinpath(self.name + suffix)
        if fname.suffix in TAB_SUFFIXES:
            self.metadata['dialect']['delimiter'] = '\t'
        assert fname.parent.exists()
        with UnicodeWriter(
                fname, delimiter=self.metadata['dialect']['delimiter']) as writer:
            writer.writerow(self.fields)
            for row in self.rows:
                writer.writerow(list(row.values()))
        self.metadata.write(fname.parent.joinpath(self.metadata_name(suffix=suffix)))
        if self.sources.bibdata.entries:
            self.sources.write(fname.parent.joinpath(self.bib_name()))
