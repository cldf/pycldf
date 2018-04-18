# coding: utf8
"""Functionality to load a set of CLDF datasets into a sqlite db.

Notes:
- Only CLDF components will be loaded into the db.
- The names of the columns in the database are the names from the CSV files, not the
  preferred labels for the corresponding CLDF properties.
"""

from __future__ import unicode_literals, print_function, division

from collections import OrderedDict, defaultdict
import sqlite3
from contextlib import closing
from json import dumps

from six import text_type

import attr
from csvw.datatypes import DATATYPES
from clldutils.path import Path, remove

from pycldf.terms import term_uri
from pycldf.sources import Sources

__all__ = ['Database']


def identity(s):
    return s


TYPE_MAP = {
    'string': ('TEXT', identity),
    'integer': ('INTEGER', identity),
    'boolean': ('INTEGER', lambda s: s if s is None else int(s)),
}
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


def insert(db, table, keys, *rows):
    if rows:
        if isinstance(keys, text_type):
            keys = [k.strip() for k in keys.split(',')]
        keys = ['"{:}"'.format(k.replace('"', r'\"')) for k in keys]
        query = "INSERT INTO \"{0}\" ({1}) VALUES ({2})".format(
            table, ','.join(keys), ','.join(['?' for _ in keys]))
        db.executemany(query, rows)


class Database(object):
    def __init__(self, fname):
        """
        A `Database` instance is initialized with a file path.

        :param fname: Path to a file in the file system where the db is to be stored.
        """
        self.fname = Path(fname)

    def drop(self):
        if self.fname.exists():
            remove(self.fname)

    def connection(self):
        return closing(sqlite3.connect(self.fname.as_posix()))

    def create(self, force=False):
        """
        Creates a db file with the core schema.

        :param force: If `True` an existing db file will be overwritten.
        """
        if self.fname and self.fname.exists():
            if force:
                self.drop()
            else:
                raise ValueError('db file already exists, use force=True to overwrite')
        with self.connection() as db:
            db.execute(
                """\
CREATE TABLE dataset (
    ID INTEGER PRIMARY KEY NOT NULL,
    name TEXT,
    module TEXT,
    metadata_json TEXT
)""")
            db.execute("""\
CREATE TABLE datasetmeta (
    dataset_ID INT ,
    key TEXT,
    value TEXT,
    FOREIGN KEY(dataset_ID) REFERENCES dataset(ID)
)""")
            db.execute("""\
CREATE TABLE SourceTable (
    dataset_ID INT ,
    ID TEXT PRIMARY KEY NOT NULL,
    bibtex_type TEXT,
    {0}
    extra TEXT,
    FOREIGN KEY(dataset_ID) REFERENCES dataset(ID)
)""".format('\n    '.join('"{0}" TEXT,'.format(f) for f in BIBTEX_FIELDS)))

    def fetchone(self, sql, conn=None):
        return self._fetch(sql, 'fetchone', conn)

    def fetchall(self, sql, conn=None):
        return self._fetch(sql, 'fetchall', conn)

    def _fetch(self, sql, method, conn):
        def _do(conn, sql, method):
            cu = conn.cursor()
            cu.execute(sql)
            return getattr(cu, method)()

        if not conn:
            with self.connection() as conn:
                return _do(conn, sql, method)
        else:
            return _do(conn, sql, method)

    def delete(self, dataset_id):
        with self.connection() as db:
            for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'"):
                table = row[0]
                if table != 'dataset':
                    db.execute(
                        "DELETE FROM {0} WHERE dataset_ID = ?".format(table),
                        (dataset_id,))
            db.execute("DELETE FROM dataset WHERE ID = ?", (dataset_id,))
            db.commit()

    def _create_table_if_not_exists(self, table):
        if table.name in [r[0] for r in self.fetchall(
                "SELECT name FROM sqlite_master WHERE type='table'")]:
            return False

        with self.connection() as conn:
            conn.execute(table.sql)
        return True

    def load(self, dataset):
        """
        Load a CLDF dataset into the database.

        :param dataset:
        :return:
        """
        tables, ref_tables = schema(dataset)

        # update the DB schema:
        for t in tables:
            if self._create_table_if_not_exists(t):
                continue
            db_cols = {r[1]: r[2] for r in self.fetchall(
                "PRAGMA table_info({0})".format(t.name))}
            for col in t.columns:
                if col.name not in db_cols:
                    with self.connection() as conn:
                        conn.execute(
                            "ALTER TABLE {0} ADD COLUMN \"{1.name}\" {1.db_type}".format(
                                t.name, col))
                else:
                    if db_cols[col.name] != col.db_type:
                        raise ValueError(
                            'column {0}:{1} {2} redefined with new type {3}'.format(
                                t.name, col.name, db_cols[col.name], col.db_type))

        for t in ref_tables.values():
            self._create_table_if_not_exists(t)

        # then load the data:
        with self.connection() as db:
            db.execute('PRAGMA foreign_keys = ON;')
            pk = max(
                [r[0] for r in self.fetchall("SELECT ID FROM dataset", conn=db)] or
                [0]) + 1
            insert(
                db,
                'dataset',
                'ID,name,module,metadata_json',
                (pk, '{0}'.format(dataset), dataset.module, dumps(dataset.metadata_dict)))
            insert(
                db,
                'datasetmeta',
                'dataset_ID,key,value',
                *[(pk, k, '{0}'.format(v)) for k, v in dataset.properties.items()])

            # load sources:
            rows = []
            for src in dataset.sources.items():
                values = [pk, src.id, src.genre] + [src.get(k) for k in BIBTEX_FIELDS]
                values.append(
                    dumps({k: v for k, v in src.items() if k not in BIBTEX_FIELDS}))
                rows.append(tuple(values))
            insert(
                db,
                'SourceTable',
                ['dataset_ID', 'ID', 'bibtex_type'] + BIBTEX_FIELDS + ['extra'],
                *rows)

            # For regular tables, we extract and keep references to sources.
            refs = defaultdict(list)

            for t in tables:
                cols = {col.name: col for col in t.columns}
                ref_table = ref_tables.get(t.name)
                rows, keys = [], []
                for row in dataset[t.name]:
                    keys, values = ['dataset_ID'], [pk]
                    for k, v in row.items():
                        if ref_table and k == ref_table.consumes:
                            refs[ref_table.name].append((row[t.primary_key], v))
                        else:
                            col = cols[k]
                            if isinstance(v, list):
                                v = (col.separator or ';').join(
                                    col.convert(vv) for vv in v)
                            else:
                                v = col.convert(v)
                            keys.append(k)
                            values.append(v)
                    rows.append(tuple(values))
                insert(db, t.name, keys, *rows)

            # Now insert the references, i.e. the associations with sources:
            for tname, items in refs.items():
                rows = []
                for oid, sources in items:
                    for source in sources:
                        sid, context = Sources.parse(source)
                        rows.append([pk, oid, sid, context])
                oid_col = '{0}_ID'.format(tname.replace('Source', ''))
                insert(
                    db,
                    tname,
                    ['dataset_ID', '{:}'.format(oid_col), 'Source_ID', 'Context'],
                    *rows)
            db.commit()


@attr.s
class ColSpec(object):
    """
    A `ColSpec` captures sufficient information about a `Column` for the DB schema.
    """
    name = attr.ib()
    csvw_type = attr.ib(default='string', convert=lambda s: s if s else 'string')
    separator = attr.ib(default=None)
    primary_key = attr.ib(default=None)
    db_type = attr.ib(default=None)
    convert = attr.ib(default=None)

    def __attrs_post_init__(self):
        if self.csvw_type in TYPE_MAP:
            self.db_type, self.convert = TYPE_MAP[self.csvw_type]
        else:
            self.db_type = 'TEXT'
            self.convert = DATATYPES[self.csvw_type].to_string

    @property
    def sql(self):
        return '"{0.name}" {0.db_type}{1}'.format(
            self, ' PRIMARY KEY NOT NULL' if self.primary_key else '')


@attr.s
class TableSpec(object):
    """
    A `TableSpec` captures sufficient information about a `Table` for the DB schema.
    """
    name = attr.ib()
    columns = attr.ib(default=attr.Factory(list))
    foreign_keys = attr.ib(default=attr.Factory(list))
    consumes = attr.ib(default=None)
    primary_key = attr.ib(default=None)

    @property
    def sql(self):
        clauses = [col.sql for col in self.columns]
        clauses.append('"dataset_ID" INTEGER NOT NULL')
        clauses.append('FOREIGN KEY("dataset_ID") REFERENCES "dataset"("ID")')
        for fk, ref, refcols in self.foreign_keys:
            clauses.append('FOREIGN KEY("{0}") REFERENCES "{1}"("{2}")'.format(
                ','.join(fk), ref, ','.join(refcols)))
        query = "CREATE TABLE \"{0}\" (\n    {1}\n)".format(
            self.name, ',\n    '.join(clauses))
        return query


def schema(ds):
    """
    Convert the table and column descriptions of a `Dataset` into specifications for the
    DB schema.

    :param ds:
    :return: A pair (tables, reference_tables).
    """
    tables, ref_tables = {}, {}
    table_lookup = {t.url.string: t for t in ds.tables}
    for table in table_lookup.values():
        try:
            sql_table_name = ds.get_tabletype(table)
        except ValueError:
            # SQLite is very permissive with table names.
            sql_table_name = table.url.string
        spec = TableSpec(sql_table_name)
        spec.primary_key = [
            c for c in table.tableSchema.columns if
            c.propertyUrl and c.propertyUrl.uri == term_uri('id')][0].name
        for c in table.tableSchema.columns:
            if c.propertyUrl and c.propertyUrl.uri == term_uri('source'):
                # A column referencing sources is replaced by an association table.
                otype = sql_table_name.replace('Table', '')
                ref_tables[sql_table_name] = TableSpec(
                    '{0}Source'.format(otype),
                    [ColSpec(otype + '_ID'), ColSpec('Source_ID'), ColSpec('Context')],
                    [
                        ([otype + '_ID'], sql_table_name, [spec.primary_key]),
                        (['Source_ID'], 'SourceTable', ['ID']),
                    ],
                    c.name)
            else:
                spec.columns.append(ColSpec(
                    c.header,
                    c.datatype.base if c.datatype else c.datatype,
                    c.separator,
                    c.header == spec.primary_key))
        for fk in table.tableSchema.foreignKeys:
            if fk.reference.schemaReference:  # pragma: no cover
                # We only support Foreign Key references between tables!
                continue
            try:
                ref = ds.get_tabletype(table_lookup[fk.reference.resource.string])
            except ValueError:
                ref = fk.reference.resource.string
            spec.foreign_keys.append((
                tuple(sorted(fk.columnReference)),
                ref,
                tuple(sorted(fk.reference.columnReference))))
        tables[spec.name] = spec

    # must determine the order in which tables must be created!
    ordered = OrderedDict()
    i = 0
    #
    # We loop through the tables repeatedly, and whenever we find one, which has all
    # referenced tables already in ordered, we move it from tables to ordered.
    #
    while tables and i < 100:
        i += 1
        for table in list(tables.keys()):
            if all(ref[1] in ordered for ref in tables[table].foreign_keys):
                # All referenced tables are already created.
                ordered[table] = tables.pop(table)
                break
    if tables:  # pragma: no cover
        raise ValueError('there seem to be cyclic dependencies between the tables')

    return list(ordered.values()), ref_tables
