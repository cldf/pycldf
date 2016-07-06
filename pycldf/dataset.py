# coding: utf8
from __future__ import unicode_literals, print_function, division
from collections import OrderedDict, Counter
import re
import logging

from clldutils.path import Path
from clldutils.dsv import reader, UnicodeWriter

from pycldf.sources import Sources
from pycldf.metadata import Metadata
from pycldf.util import MD_SUFFIX, Archive, Row, TAB_SUFFIXES

__all__ = ['Dataset']

logging.basicConfig()
log = logging.getLogger(__name__)
NAME_PATTERN = re.compile('^[a-zA-Z\-_0-9]+$')
REQUIRED_FIELDS = [('ID',), ('Language_ID',), ('Parameter_ID', 'Feature_ID'), ('Value',)]


class ValuesRow(Row):
    def __init__(self, dataset):
        self.dataset = dataset
        Row.__init__(self, dataset.table.schema)

    @property
    def refs(self):
        return list(self.dataset.sources.expand_refs(self.get('Source', '')))


class Dataset(object):
    """
    API to access a CLDF dataset.
    """
    def __init__(self, name):
        assert NAME_PATTERN.match(name)
        self.name = name
        self.sources = Sources()
        self.metadata = Metadata()
        self._rows = OrderedDict()

        # We store the fields (a.k.a. header) as tuple because it must be immutable after
        # first assignment (since changing is not well defined when there are already
        # rows).
        self._fields = ()
        self._source_count = None
        self._cited_sources = set()
        self._table = None

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self.name)

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

    @property
    def table(self):
        return self._table

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
        table = self.metadata.get_table()
        if table:
            assert list(value) == list(table.schema.columns.keys())
        else:
            table = self.metadata.add_table(
                'values',
                '',
                [{'name': col, 'datatype': 'string'} for col in value])
            table.schema.primaryKey = 'ID'
        self._table = table
        self._fields = value

    @property
    def rows(self):
        return list(self._rows.values())

    @property
    def stats(self):
        return dict(
            languages=set(row['Language_ID'] for row in self.rows),
            parameters=set(row['Parameter_ID'] for row in self.rows),
            rowcount=(
                len(self),
                sum([1 for row in self.rows
                     if row['Language_ID'] and row['Parameter_ID']])),
            values=Counter(row['Value'] for row in self.rows),
        )

    def add_row(self, row):
        if not row:
            return

        d = ValuesRow.from_list(self, row)
        if d['ID'] in self._rows:
            raise ValueError('duplicate row ID: %s' % d['ID'])
        for ref in self.sources.expand_refs(d.get('Source', '')):
            self._cited_sources.add(ref.source.id)
        self._rows[d['ID']] = d
        return d

    @staticmethod
    def filename(fname, type_):
        """
        Compute the path for optional CLDF files relative to a given values file.

        :param fname: Path of the values file
        :param type_: Type of the optional file
        :return: name of the optional file
        """
        if type_ == 'sources':
            return fname.stem + '.bib'
        if type_ == 'metadata':
            return fname.stem + fname.suffix + MD_SUFFIX
        raise ValueError(type_)  # pragma: no cover

    @staticmethod
    def _existing_file(fname):
        fname = Path(fname)
        assert fname.exists() and fname.is_file()
        return fname

    @classmethod
    def _from(cls, data, container=None, skip_on_error=False):
        container = container or data.parent
        dataset = cls(data.stem)
        dataset.metadata.read(Dataset.filename(data, 'metadata'), container)
        dataset._table = dataset.metadata.get_table()
        dataset.sources.read(Dataset.filename(data, 'sources'), container)
        delimiter = ','
        if dataset.table:
            delimiter = dataset.table.dialect.delimiter
        if data.suffix in TAB_SUFFIXES:
            delimiter = '\t'

        if isinstance(container, Archive):
            rows = container.read_text(data.name).split('\n')
        else:
            rows = data

        for i, row in enumerate(reader(rows, delimiter=delimiter)):
            if i == 0:
                dataset.fields = tuple(row)
            else:
                try:
                    dataset.add_row(row)
                except ValueError as e:
                    if skip_on_error:
                        log.warn('skipping row in line %s: %s' % (i + 1, e))
                    else:
                        raise e
        dataset.table.dialect.delimiter = delimiter
        dataset.table.url = data.name
        return dataset

    @classmethod
    def from_zip(cls, fname, name=None):
        archive = Archive(cls._existing_file(fname))
        return cls._from(
            Path(archive.metadata_name(prefix=name)[:-len(MD_SUFFIX)]), archive)

    @classmethod
    def from_metadata(cls, fname, container=None):
        fname = Path(fname)
        if not fname.name.endswith(MD_SUFFIX):
            raise ValueError('metadata file name must end with %s' % MD_SUFFIX)
        return cls._from(
            fname.parent.joinpath(fname.name[:-len(MD_SUFFIX)]), container=container)

    @classmethod
    def from_file(cls, fname, skip_on_error=False):
        """
        Factory method to create a `Dataset` from a CLDF values file.

        :param fname: Path of the CLDF values file.
        :return: `Dataset` instance.
        """
        return cls._from(cls._existing_file(fname), skip_on_error=skip_on_error)

    def write(self, outdir='.', suffix='.csv', cited_sources_only=False, archive=False):
        outdir = Path(outdir)
        if not outdir.exists():
            raise ValueError(outdir.as_posix())

        close = False
        if archive:
            if isinstance(archive, Archive):
                container = archive
            else:
                container = Archive(outdir.joinpath(self.name + '.zip'), mode='w')
                close = True
        else:
            container = outdir

        fname = Path(outdir).joinpath(self.name + suffix)
        if fname.suffix in TAB_SUFFIXES:
            self.table.dialect.delimiter = '\t'

        with UnicodeWriter(
                None if isinstance(container, Archive) else fname,
                delimiter=self.table.dialect.delimiter) as writer:
            writer.writerow(self.fields)
            for row in self.rows:
                writer.writerow(row.to_list())

        if isinstance(container, Archive):
            container.write_text(writer.read(), fname.name)
        self.table.url = fname.name

        self.metadata.write(Dataset.filename(fname, 'metadata'), container)
        ids = self._cited_sources if cited_sources_only else None
        self.sources.write(Dataset.filename(fname, 'sources'), container, ids=ids)
        if close:
            container.close()
