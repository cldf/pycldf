# coding: utf8
from __future__ import unicode_literals, print_function, division
from collections import OrderedDict
import re

from uritemplate import expand
from clldutils.path import Path
from clldutils.dsv import reader, UnicodeWriter

from pycldf.sources import Sources
from pycldf.metadata import Metadata


NAME_PATTERN = re.compile('^[a-zA-Z\-_0-9]+$')
TAB_SUFFIXES = ['.tsv', '.tab']
REQUIRED_FIELDS = [('ID',), ('Language_ID',), ('Parameter_ID', 'Feature_ID'), ('Value',)]


class Row(OrderedDict):
    def __init__(self, dataset):
        self.dataset = dataset
        OrderedDict.__init__(self)

    @property
    def url(self):
        if 'aboutUrl' in self.dataset.metadata.values_table['tableSchema']:
            return expand(
                self.dataset.metadata.values_table['tableSchema']['aboutUrl'], self)

    def expand(self, field):
        if field == 'Source':
            return list(self.dataset.sources.expand_refs(self[field]))
        col_spec = self.dataset.metadata.get_column('values', field, {})
        if 'valueUrl' in col_spec:
            return expand(col_spec['valueUrl'], self)
        return self[field]


class Dataset(object):
    """
    API to access a CLDF dataset.
    """
    def __init__(self, name, sources=None, metadata=None):
        assert NAME_PATTERN.match(name)
        if sources:
            assert isinstance(sources, Sources)
        if metadata:
            assert isinstance(metadata, Metadata)
        self.name = name
        self.sources = sources or Sources()
        self.metadata = metadata or Metadata()
        self._rows = OrderedDict()

        # We store the fields (a.k.a. header) as tuple because it must be immutable after
        # first assignment (since changing is not well defined when there are already
        # rows).
        self._fields = ()
        self._source_count = None
        self._cited_sources = set()

    def __len__(self):
        """The length of a dataset is the number of rows in the values file."""
        return len(self.rows)

    def __getitem__(self, item):
        """
        Individual rows can be accessed by integer index or by row ID.

        :param item: `int` to access row by index, `str` to access by row ID
        :return: `OrderedDict`
        """
        if isinstance(item, int):
            return self.rows[item]
        return self._rows[item]

    @property
    def fields(self):
        """
        Read-only property to access the fields (a.k.a. header) defined for the dataset.

        :return: `tuple` of field names
        """
        return self._fields

    @fields.setter
    def fields(self, value):
        """
        Fields can be assigned (but only once) for a dataset.

        :param value: `tuple` of field names.
        """
        if self._fields:
            raise ValueError('fields can only be assigned once!')
        assert isinstance(value, tuple)
        assert all(any(field in value for field in variants)
                   for variants in REQUIRED_FIELDS)
        table = self.metadata.values_table
        if table:
            assert list(value) == [
                col['name'] for col in table['tableSchema']['columns']]
        else:
            table = self.metadata.add_table(
                'values',
                '',
                [{'name': col, 'datatype': 'string'} for col in value])
            table['tableSchema']['primaryKey'] = 'ID'
        self._fields = value

    @property
    def rows(self):
        return list(self._rows.values())

    def add_row(self, row):
        assert len(row) == len(self.fields)
        d = Row(self)
        for k, v in zip(self.fields, row):
            d[k] = v
        assert d['ID'] not in self._rows
        for ref in self.sources.expand_refs(d.get('Source', '')):
            self._cited_sources.add(ref.source.id)
        self._rows[d['ID']] = d
        return d

    @staticmethod
    def path(fname, type_):
        """
        Compute the path for optional CLDF files relative to a given values file.

        :param fname: Path of the values file
        :param type_: Type of the optional file
        :return: Path of the optional file
        """
        if type_ == 'sources':
            return fname.parent.joinpath(fname.stem + '.bib')
        if type_ == 'metadata':
            return fname.parent.joinpath(fname.stem + fname.suffix + '-metadata.json')
        raise ValueError(type_)  # pragma: no cover

    @classmethod
    def from_file(cls, fname):
        """
        Factory method to create a `Dataset` from a CLDF values file.

        :param fname: Path of the CLDF values file.
        :return: `Dataset` instance.
        """
        fname = Path(fname)
        assert fname.exists() and fname.is_file()
        dataset = cls(
            fname.stem,
            metadata=Metadata.from_file(Dataset.path(fname, 'metadata')),
            sources=Sources.from_file(Dataset.path(fname, 'sources')))
        if fname.suffix in TAB_SUFFIXES:
            dataset.metadata.dialect['delimiter'] = '\t'
        for i, row in enumerate(reader(
            fname, delimiter=dataset.metadata.dialect.get('delimiter', ',')
        )):
            if i == 0:
                dataset.fields = tuple(row)
            else:
                dataset.add_row(row)
        dataset.metadata.values_table['url'] = fname.name
        return dataset

    def write(self, outdir='.', suffix='.csv', cited_sources_only=False):
        fname = Path(outdir).joinpath(self.name + suffix)
        if fname.suffix in TAB_SUFFIXES:
            self.metadata.dialect['delimiter'] = '\t'
        assert fname.parent.exists()
        with UnicodeWriter(
                fname, delimiter=self.metadata.dialect['delimiter']) as writer:
            writer.writerow(self.fields)
            for row in self.rows:
                writer.writerow(list(row.values()))
        self.metadata.write(Dataset.path(fname, 'metadata'))
        ids = self._cited_sources if cited_sources_only else None
        self.sources.write(Dataset.path(fname, 'sources'), ids=ids)
