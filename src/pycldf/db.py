"""
Functionality to load a CLDF dataset into a sqlite db.

To make the resulting SQLite database useful without access to the datasets metadata, we
use terms of the CLDF ontology for database objects as much as possible, i.e.
- table names are component names (e.g. ``ValueTable`` for a table with `propertyUrl` \
  ``http://cldf.clld.org/v1.0/terms.rdf#ValueTable``)
- column names are property names, prefixed with "cldf" + UNDERSCORE (e.g. a column with \
  `propertyUrl` ``http://cldf.clld.org/v1.0/terms.rdf#id`` will be ``cldf_id`` in the database)

This naming scheme also extends to automatically created association tables. I.e. when a
table specifies a list-valued foreign key, an association table is created to implement this
many-to-many relationship. The name of the association table is the concatenation of
- the url properties of the tables in this relationship or of
- the component names of the tables in the relationship.

E.g. a list-valued foreign key from `FormTable` to `ParameterTable` will result in an
association table

.. code-block:: sql

  CREATE TABLE `FormTable_ParameterTable` (
    `FormTable_cldf_id` TEXT,
    `ParameterTable_cldf_id` TEXT,
    `context` TEXT,
    FOREIGN KEY(`FormTable_cldf_id`) REFERENCES `FormTable`(`cldf_id`) ON DELETE CASCADE,
    FOREIGN KEY(`ParameterTable_cldf_id`) REFERENCES `ParameterTable`(`cldf_id`) ON DELETE CASCADE
  );

while a list-valued foreign key to a custom table may result in something like this

.. code-block:: sql

  CREATE TABLE `FormTable_custom.csv` (
    `FormTable_cldf_id` TEXT,
    `custom.csv_id` TEXT,
    `context` TEXT,
    FOREIGN KEY(`FormTable_cldf_id`) REFERENCES `FormTable`(`cldf_id`) ON DELETE CASCADE,
    FOREIGN KEY(`custom.csv_id`) REFERENCES `custom.csv`(`id`) ON DELETE CASCADE
  );
"""
import typing
import pathlib
import functools
import collections

import attr
import csvw
import csvw.db

from pycldf.terms import TERMS
from pycldf.sources import Reference, Sources, Source
from pycldf import Dataset

__all__ = ['Database']

PRIMARY_KEY_NAMES = ['id', 'pk']
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
    """
    Specifies column name translations for a table.
    """
    name = attr.ib(default=None)
    columns = attr.ib(default=attr.Factory(dict))


def translate(d: typing.Dict[str, TableTranslation], table: str, col=None) -> str:
    """
    Translate a db object name.

    :param d: ``dict`` mapping table urls to `TableTranslation` instances.
    :param table: The table name of the object to be translated.
    :param col: Column name to be translated or `None` - so ``table`` will be translated.
    :return: Translated name.
    """
    if col:
        if table in d and col in d[table].columns:
            # A simple, translateable column name.
            return d[table].columns[col]
        if '_' in col:
            parts = col.split('_')
            t = '_'.join(parts[:-1])
            c = parts[-1]
            if t in table and t in d and c in d[t].columns:
                # A generated column name of an association table.
                return '_'.join([d[t].name or t, d[t].columns[c]])
        return col

    # To handle association tables, we proceed as follows:
    # 1. We split the full table name on underscores - the separators of sub-table names in
    # association tables.
    # 2. Since regular table names may contain underscores as well, we try to find the longest
    # concatenation of _-separated name parts which appears in the translation dict.
    # 3. We repeat step 2 until all name parts have been consumed.
    def t(n):
        if n in d:
            return d[n].name or n
        tables, comps = [], n.split('_')
        while comps and len(comps) > 1:
            for i in range(len(comps), 0, -1):
                nc = '_'.join(comps[:i])
                if nc in d:
                    tables.append(d[nc].name or nc)
                    comps = comps[i:]
                    break
            else:
                raise ValueError(n)  # pragma: no cover
        if comps:
            assert len(comps) == 1
            tables.append(d[comps[0]].name or comps[0] if comps[0] in d else comps[0])
        return '_'.join(tables)

    return t(table)


def clean_bibtex_key(s):
    return s.replace('-', '_').lower()


class Database(csvw.db.Database):
    """
    Extend the functionality provided by ``csvw.db.Database`` by

    - providing consistent naming of schema objects according to CLDF semantics,
    - integrating sources into the DB schema.
    """
    source_table_name = 'SourceTable'

    def __init__(self, dataset: Dataset, **kw):
        """
        :param dataset: The :class:`Dataset` instance from which to derive the database schema.
        """
        self.dataset = dataset
        self._retranslate = collections.defaultdict(dict)
        self._source_cols = ['id', 'genre'] + BIBTEX_FIELDS

        infer_primary_keys = kw.pop('infer_primary_keys', False)

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
            except (KeyError, ValueError):
                # If no table type can be determined, there's nothing to translate.
                pass
            new_col_names = []
            for col in table.tableSchema.columns:
                if col.propertyUrl and col.propertyUrl.uri in TERMS.by_uri:
                    # Translate local column names to local names of CLDF Ontology terms, prefixed
                    # with `cldf_`:
                    col_name = 'cldf_{0.name}'.format(TERMS.by_uri[col.propertyUrl.uri])
                    new_col_names.append(col_name.lower())
                    translations[table.local_name].columns[col.header] = col_name
                    self._retranslate[table.local_name][col_name] = col.header

            for col in table.tableSchema.columns:
                if not (col.propertyUrl and col.propertyUrl.uri in TERMS.by_uri):
                    if col.header.lower() in new_col_names:
                        # A name clash! We translate the old column name!
                        col_name = '_{}'.format(col.header)
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
            if not table.tableSchema.primaryKey and infer_primary_keys:
                for col in table.tableSchema.columns:
                    if col.name.lower() in PRIMARY_KEY_NAMES:
                        table.tableSchema.primaryKey = [col.name]
                        break
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
                        tl = translations[table.local_name]
                        translations['{0}_{1}'.format(table.local_name, self.source_table_name)] = \
                            TableTranslation(
                                name='{0}_{1}'.format(tl.name, self.source_table_name),
                                columns={'{0}_{1}'.format(
                                    table.local_name, table.tableSchema.primaryKey[0],
                                ): '{0}_{1}'.format(
                                    tl.name, tl.columns[table.tableSchema.primaryKey[0]],
                                )})
                    break

        # Make sure `base` directory can be resolved:
        tg._fname = dataset.tablegroup._fname
        csvw.db.Database.__init__(
            self, tg, translate=functools.partial(translate, translations), **kw)

    def association_table_context(self, table, column, fkey):
        if self.translate(table.name, column) == 'cldf_source':
            # We decompose references into the source ID and optional pages. Pages are stored as
            # `context` of the association table and composed again in `select_many_to_many`.
            if '[' in fkey:
                assert fkey.endswith(']')
                fkey, _, rem = fkey.partition('[')
                return fkey, rem[:-1]
            return fkey, None
        return csvw.db.Database.association_table_context(
            self, table, column, fkey)  # pragma: no cover

    def select_many_to_many(self, db, table, context):
        if table.name.endswith('_' + self.source_table_name):
            atable = table.name.partition('_' + self.source_table_name)[0]
            if self.translate(atable, context) == 'cldf_source':
                # Compose references:
                res = csvw.db.Database.select_many_to_many(self, db, table, None)
                return {k: ['{0}'.format(Reference(*vv)) for vv in v] for k, v in res.items()}
        return csvw.db.Database.select_many_to_many(self, db, table, context)  # pragma: no cover

    def write(self, _force=False, _exists_ok=False, **items):
        if self.fname and self.fname.exists():
            if _force:
                self.fname.unlink()
            elif _exists_ok:
                raise NotImplementedError()
        return csvw.db.Database.write(
            self, _force=False, _exists_ok=False, _skip_extra=True, **items)

    def write_from_tg(self, _force: bool = False, _exists_ok: bool = False):
        """
        Write the data from `self.dataset` to the database.
        """
        items = {
            tname: list(t.iterdicts())
            for tname, t in self.tg.tabledict.items() if tname != self.source_table_name}
        items[self.source_table_name] = []
        for src in self.dataset.sources:
            item = collections.OrderedDict([(k, '') for k in self._source_cols])
            item.update({clean_bibtex_key(k): v for k, v in src.items()})
            item.update({'id': src.id, 'genre': src.genre})
            items[self.source_table_name].append(item)
        return self.write(_force=_force, _exists_ok=_exists_ok, **items)

    def query(self, sql: str, params=None) -> list:
        """
        Run `sql` on the database, returning the list of results.
        """
        with self.connection() as conn:
            cu = conn.execute(sql, params or ())
            return list(cu.fetchall())

    def retranslate(self, table, item):
        return {self._retranslate.get(table.local_name, {}).get(k, k): v for k, v in item.items()}

    @staticmethod
    def round_geocoordinates(item, precision=4):
        """
        We round geo coordinates to ``precision`` decimal places.

        .. seealso:: `<https://en.wikipedia.org/wiki/Decimal_degrees>`_

        :param item:
        :param precision:
        :return: item
        """
        for attr_ in ['cldf_latitude', 'cldf_longitude']:
            if item.get(attr_):
                item[attr_] = round(item[attr_], precision)
        return item

    def to_cldf(self, dest, mdname='cldf-metadata.json', coordinate_precision=4) -> pathlib.Path:
        """
        Write the data from the db to a CLDF dataset according to the metadata in `self.dataset`.

        :param dest: Destination directory for the CLDF data.
        :param mdname: Name to use for the CLDF metadata file.
        :return: path of the metadata file
        """
        dest = pathlib.Path(dest)
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
