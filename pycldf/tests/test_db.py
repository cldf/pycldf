# coding: utf8
from __future__ import unicode_literals, print_function, division
from sqlite3 import IntegrityError

from clldutils.testing import WithTempDir
from clldutils.csvw.metadata import Column
from clldutils.csvw.datatypes import anyURI

from pycldf.dataset import Dataset, Dictionary, StructureDataset
from pycldf.tests.util import FIXTURES


class Tests(WithTempDir):
    def _make_db(self, fname='db.sqlite'):
        from pycldf.db import Database

        return Database(self.tmp_path(fname))

    def test_db(self):
        ds = Dataset.from_metadata(FIXTURES.joinpath('ds1.csv-metadata.json'))
        db = self._make_db()
        db.create()
        db.load(ds)
        self.assertEqual(len(db.fetchall("SELECT name FROM dataset")), 1)
        with self.assertRaises(IntegrityError):
            db.load(ds)
        db.delete(db.fetchone("SELECT ID FROM dataset")[0])
        db.load(ds)
        db.drop()

    def test_create(self):
        db = self._make_db()
        db.create()
        with self.assertRaises(ValueError):
            db.create()
        db.create(force=True)

    def test_update(self):
        ds = Dictionary.in_dir(self.tmp_path('d1'))
        ds.write(EntryTable=[], SenseTable=[])
        ds2 = Dictionary.in_dir(self.tmp_path('d2'))
        ds2.write(EntryTable=[], SenseTable=[])
        db = self._make_db()
        db.create()
        db.load(ds)
        db.load(ds2)
        ds.tables[0].tableSchema.columns.append(Column(name='newcol', datatype='integer'))
        db.load(ds)
        ds.tables[0].tableSchema.columns[-1].datatype.base = 'string'
        with self.assertRaises(ValueError):
            db.load(ds)

    def test_newcol(self):
        ds = StructureDataset.in_dir(self.tmp_path('d'))

        # We rename the ID column of the ValueTable:
        ds['ValueTable', 'ID'].name = 'idx'
        ds['ValueTable'].tableSchema.columns.extend([
            Column(name='col1', datatype='anyURI'),
            Column(name='col2', datatype='integer'),
            Column(name='col3'),
        ])
        ds.write(ValueTable=[{
            'idx': '1',
            'Language_ID': 'l',
            'Parameter_ID': 'p',
            'Value': 'v',
            'Source': ['meier2015'],
            'col2': 5,
            'col1': anyURI().to_python('http://example.org')}])
        db = self._make_db()
        db.create()
        with self.assertRaises(IntegrityError):  # A missing source is referenced!
            db.load(ds)
        ds.add_sources("@misc{meier2015,\ntitle={title}\n}")
        db.load(ds)
        self.assertEqual(
            db.fetchone("""\
select
  s.title
from
  SourceTable as s, ValueSource as vs, ValueTable as v
where
  s.id = vs.source_id and vs.value_id = v.idx and v.idx = 1""")[0],
            'title')
        self.assertEqual(
            db.fetchone("select col1 from valuetable")[0], 'http://example.org')
        self.assertEqual(
            db.fetchone("select col2 from valuetable")[0], 5)
