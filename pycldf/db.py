# coding: utf8
"""
Functionality to load a set of CLDF datasets into a sqlite db.
"""
from __future__ import unicode_literals, print_function, division
from collections import OrderedDict
import sqlite3
from contextlib import closing
from json import dumps

import attr
from clldutils.path import Path, remove
from clldutils.csvw.datatypes import DATATYPES


def identity(s):
    return s


TYPE_MAP = {
    'string': ('TEXT', identity),
    'integer': ('INTEGER', identity),
    'boolean': ('INTEGER', lambda s: s if s is None else int(s)),
}


class Database(object):
    def __init__(self, fname):
        self.fname = Path(fname)

    def drop(self):
        if self.fname.exists():
            remove(self.fname)

    def connection(self):
        return closing(sqlite3.connect(self.fname.as_posix()))

    def create(self):
        if self.fname and self.fname.exists():
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

    def load(self, dataset):
        tables = schema(dataset)

        # update the DB schema:
        db_tables = [
            r[0] for r in self.fetchall(
                "SELECT name FROM sqlite_master WHERE type='table'")]
        for t in tables:
            if t.name not in db_tables:
                with self.connection() as conn:
                    conn.execute(t.sql)
                continue
            db_cols = {r[1]: r[2] for r in self.fetchall(
                "PRAGMA table_info({0})".format(t.name))}
            for col in t.columns:
                if col.name not in db_cols:
                    with self.connection() as conn:
                        conn.execute(
                            "ALTER TABLE {0} ADD COLUMN `{1.name}` {1.db_type}".format(
                                t.name, col))
                else:
                    if db_cols[col.name] != col.db_type:
                        raise ValueError(
                            'column {0}:{1} {2} redefined with new type {3}'.format(
                                t.name, col.name, db_cols[col.name], col.db_type))

        # then load the data:
        with self.connection() as db:
            db.execute('PRAGMA foreign_keys = ON;')
            pk = max(
                [r[0] for r in self.fetchall("SELECT ID FROM dataset", conn=db)] or
                [0]) + 1
            db.execute(
                "INSERT INTO dataset (ID,name,module,metadata_json) VALUES (?,?,?,?)",
                (pk, '{0}'.format(dataset), dataset.module, dumps(dataset.metadata_dict)))
            # json.dumps(dataset.metadata_dict)
            db.executemany(
                "INSERT INTO datasetmeta (dataset_ID,key,value) VALUES (?,?,?)",
                [(pk, k, '{0}'.format(v)) for k, v in dataset.properties.items()])
            for t in tables:
                cols = {col.name: col for col in t.columns}
                table = dataset[t.name]
                if table:
                    rows = []
                    for row in table:
                        keys, values = ['dataset_ID'], [pk]
                        for k, v in row.items():
                            col = cols[k]
                            if isinstance(v, list):
                                v = (col.separator or ';').join(
                                    col.convert(vv) for vv in v)
                            else:
                                v = col.convert(v)
                            keys.append("`{0}`".format(k))
                            values.append(v)
                        rows.append(tuple(values))
                    if rows:
                        db.executemany(
                            "INSERT INTO {0} ({1}) VALUES ({2})".format(
                                t.name, ','.join(keys), ','.join(['?' for _ in keys])),
                            rows)
            db.commit()


@attr.s
class ColSpec(object):
    name = attr.ib()
    csvw_type = attr.ib()
    separator = attr.ib()
    primary_key = attr.ib()
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
        return '`{0.name}` {0.db_type}{1}'.format(
            self, ' PRIMARY KEY NOT NULL' if self.primary_key else '')


@attr.s
class TableSpec(object):
    name = attr.ib()
    primary_key = attr.ib()
    columns = attr.ib(default=attr.Factory(list))
    foreign_keys = attr.ib(default=attr.Factory(list))

    @property
    def sql(self):
        clauses = [col.sql for col in self.columns]
        clauses.append('dataset_ID INTEGER NOT NULL')
        clauses.append('FOREIGN KEY(dataset_ID) REFERENCES dataset(ID)')
        for fk, ref, refcols in self.foreign_keys:
            clauses.append('FOREIGN KEY({0}) REFERENCES {1}({2})'.format(
                ','.join(fk), ref, ','.join(refcols)))
        return "CREATE TABLE {0} (\n    {1}\n)".format(self.name, ',\n    '.join(clauses))


def schema(ds):
    tables = {}
    table_lookup = {t.url.string: t for t in ds.tables if ds.get_tabletype(t)}
    for table in table_lookup.values():
        spec = TableSpec(ds.get_tabletype(table), table.tableSchema.primaryKey or [])
        for c in table.tableSchema.columns:
            spec.columns.append(ColSpec(
                c.header, c.datatype.base, c.separator, c.header in spec.primary_key))
        for fk in table.tableSchema.foreignKeys:
            if fk.reference.schemaReference:
                # We only support Foreign Key references between tables!
                continue  # pragma: no cover
            ref = table_lookup[fk.reference.resource.string]
            if ds.get_tabletype(ref):
                spec.foreign_keys.append((
                    tuple(sorted(fk.columnReference)),
                    ds.get_tabletype(table_lookup[fk.reference.resource.string]),
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

    return list(ordered.values())
