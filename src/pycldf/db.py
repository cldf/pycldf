# coding: utf8
"""Functionality to load a set of CLDF datasets into a sqlite db.

Notes:
- Only CLDF components will be loaded into the db.
- The names of the columns in the database are the names from the CSV files, not the
  preferred labels for the corresponding CLDF properties.
"""

from __future__ import unicode_literals, print_function, division
import functools
import collections

import attr
import csvw
import csvw.db
from clldutils.path import Path

from pycldf.terms import TERMS
from pycldf.sources import Reference, Sources, Source

BIBTEX_FIELDS = [
    'address',  # Publisher's address
    'annote',  # An annotation for annotated bibliography styles (not typical)
    'author',  # The name(s) of the author(s) (separated by and)
    'booktitle',  # The title of the book, if only part of it is being cited
    'chapter',  # The chapter number
    'crossref',  # The key of the cross-referenced entry
    'edition',  # The edition of a book, long form (such as "First" or "Second")
    'editor',  # The name(s) of the editor(s)
    'eprint',  # A specification of electronic publication, preprint or technical report
    'howpublished',  # How it was published, if the publishing method is nonstandard
    'institution',  # institution involved in the publishing,not necessarily the publisher
    'journal',  # The journal or magazine the work was published in
    'key',  # A hidden field used for specifying or overriding the orderalphabetical order
    'month',  # The month of publication (or, if unpublished, the month of creation)
    'note',  # Miscellaneous extra information
    'number',  # The "(issue) number" of a journal, magazine, or tech-report
    'organization',  # The conference sponsor
    'pages',  # Page numbers, separated either by commas or double-hyphens.
    'publisher',  # The publisher's name
    'school',  # The school where the thesis was written
    'series',  # The series of books the book was published in
    'title',  # The title of the work
    'type',  # The field overriding the default type of publication
    'url',  # The WWW address
    'volume',  # The volume of a journal or multi-volume book
    'year',
]


@attr.s
class TableTranslation(object):
    name = attr.ib(default=None)
    columns = attr.ib(default={})


def translate(d, table, col=None):
    if col:
        if table in d and col in d[table].columns:
            return d[table].columns[col]
        return col
    if table in d and d[table].name:
        return d[table].name
    return table


def clean_bibtex_key(s):
    return s.replace('-', '_')


class Database(csvw.db.Database):
    source_table_name = 'SourceTable'

    def __init__(self, dataset, **kw):
        self.dataset = dataset
        self._retranslate = collections.defaultdict(dict)
        self._source_cols = ['id', 'genre'] + BIBTEX_FIELDS

        # We create a derived TableGroup, adding a table for the sources.
        tg = csvw.TableGroup.fromvalue(dataset.metadata_dict)

        # Assemble the translation function:
        translations = {}
        for table in dataset.tables:
            translations[table.local_name] = TableTranslation()
            try:
                tt = dataset.get_tabletype(table)
                if tt:
                    # Translate table URLs to CLDF component names:
                    translations[table.local_name].name = tt
            except KeyError:  # pragma: no cover
                pass
            for col in table.tableSchema.columns:
                if col.propertyUrl and col.propertyUrl.uri in TERMS.by_uri:
                    # Translate local column names to local names of CLDF Ontology terms, prefixed
                    # with `cldf_`:
                    col_name = 'cldf_{0.name}'.format(TERMS.by_uri[col.propertyUrl.uri])
                    translations[table.local_name].columns[col.header] = col_name
                    self._retranslate[table.local_name][col_name] = col.header

        # Add source table:
        for src in self.dataset.sources:
            for key in src:
                key = clean_bibtex_key(key)
                if key not in self._source_cols:
                    self._source_cols.append(key)

        tg.tables.append(csvw.Table.fromvalue({
            'url': self.source_table_name,
            'tableSchema': {
                'columns': [dict(name=n) for n in self._source_cols],
                'primaryKey': 'id'
            }
        }))
        tg.tables[-1]._parent = tg

        # Add foreign keys to source table:
        for table in tg.tables[:-1]:
            for col in table.tableSchema.columns:
                if col.propertyUrl and col.propertyUrl.uri == TERMS['source'].uri:
                    table.tableSchema.foreignKeys.append(csvw.ForeignKey.fromdict({
                        'columnReference': [col.header],
                        'reference': {
                            'resource': self.source_table_name,
                            'columnReference': 'id'
                        }
                    }))
                    if translations[table.local_name].name:
                        translations['{0}_{1}'.format(table.local_name, self.source_table_name)] = \
                            TableTranslation(name='{0}_{1}'.format(
                                translations[table.local_name].name, self.source_table_name))
                    break

        # Make sure `base` directory can be resolved:
        tg._fname = dataset.tablegroup._fname
        csvw.db.Database.__init__(
            self, tg, translate=functools.partial(translate, translations), **kw)

    def association_table_context(self, table, column, fkey):
        if self.translate(table.name, column) == 'cldf_source':
            if '[' in fkey:
                assert fkey.endswith(']')
                fkey, _, rem = fkey.partition('[')
                return fkey, rem[:-1]
            return fkey, None
        return csvw.db.Database.association_table_context(
            self, table, column, fkey)  # pragma: no cover

    def select_many_to_many(self, db, table, context):
        if self.translate(table.name, context) == 'cldf_source':
            cu = db.execute(
                """\
SELECT `{0}`, group_concat(`{1}`, '|'), group_concat(coalesce(context, ''), '|')
FROM `{2}` GROUP BY `{0}`""".format(
                    table.columns[0].name,
                    table.columns[1].name,
                    self.translate(table.name)))
            res = {}
            for r in cu.fetchall():
                res[r[0]] = [
                    '{0}'.format(Reference(*p)) for p in zip(r[1].split('|'), r[2].split('|'))]
            return res
        return csvw.db.Database.select_many_to_many(self, db, table, context)  # pragma: no cover

    def write(self, _force=False, _exists_ok=False, **items):
        if self.fname and self.fname.exists():
            if _force:
                self.fname.unlink()
            elif _exists_ok:
                raise NotImplementedError()
        return csvw.db.Database.write(self, _force=False, _exists_ok=False, **items)

    def write_from_tg(self, _force=False, _exists_ok=False):
        items = {
            tname: list(t.iterdicts())
            for tname, t in self.tg.tabledict.items() if tname != self.source_table_name}
        items[self.source_table_name] = []
        for src in self.dataset.sources:
            item = {k: '' for k in self._source_cols}
            item.update({clean_bibtex_key(k): v for k, v in src.items()})
            item.update({'id': src.id, 'genre': src.genre})
            items[self.source_table_name].append(item)
        return self.write(_force=_force, _exists_ok=_exists_ok, **items)

    def query(self, sql, params=None):
        with self.connection() as conn:
            cu = conn.execute(sql, params or ())
            return list(cu.fetchall())

    def retranslate(self, table, item):
        return {self._retranslate.get(table.local_name, {}).get(k, k): v for k, v in item.items()}

    @staticmethod
    def round_geocoordinates(item, precision=4):
        """
        We round geo coordinates to `precision` decimal places.

        See https://en.wikipedia.org/wiki/Decimal_degrees

        :param item:
        :param precision:
        :return: item
        """
        for attr_ in ['cldf_latitude', 'cldf_longitude']:
            if item.get(attr_):
                item[attr_] = round(item[attr_], precision)
        return item

    def to_cldf(self, dest, mdname='cldf-metadata.json', coordinate_precision=4):
        """
        Write the data from the db to a CLDF dataset according to the metadata in `self.dataset`.

        :param dest:
        :param mdname:
        :return: path of the metadata file
        """
        dest = Path(dest)
        if not dest.exists():
            dest.mkdir()

        data = self.read()

        if data[self.source_table_name]:
            sources = Sources()
            for src in data[self.source_table_name]:
                sources.add(Source(
                    src['genre'],
                    src['id'],
                    **{k: v for k, v in src.items() if k not in ['id', 'genre']}))
            sources.write(dest / self.dataset.properties.get('dc:source', 'sources.bib'))

        for table_type, items in data.items():
            try:
                table = self.dataset[table_type]
                items = [
                    self.round_geocoordinates(item, precision=coordinate_precision)
                    for item in items]
                table.common_props['dc:extent'] = table.write(
                    [self.retranslate(table, item) for item in items],
                    base=dest)
            except KeyError:
                assert table_type == self.source_table_name, table_type
        return self.dataset.write_metadata(dest / mdname)
