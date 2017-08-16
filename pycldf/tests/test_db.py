# coding: utf8
from __future__ import unicode_literals, print_function, division
from sqlite3 import IntegrityError

from clldutils.testing import WithTempDir
from clldutils.csvw.metadata import Column
from clldutils.csvw.datatypes import anyURI

from pycldf.dataset import Dataset, Dictionary, StructureDataset
from pycldf.tests.util import FIXTURES


class Tests(WithTempDir):
    def test_db(self):
        from pycldf.db import Database

        ds = Dataset.from_metadata(FIXTURES.joinpath('ds1.csv-metadata.json'))
        db = Database(self.tmp_path('db.sqlite'))
        db.create()
        with self.assertRaises(ValueError):
            db.create()
        db.load(ds)
        self.assertEqual(len(db.fetchall("SELECT name FROM dataset")), 1)
        with self.assertRaises(IntegrityError):
            db.load(ds)
        db.delete(db.fetchone("SELECT ID FROM dataset")[0])
        db.load(ds)
        db.drop()

    def test_update(self):
        from pycldf.db import Database

        ds = Dictionary.in_dir(self.tmp_path('d1'))
        ds.write(EntryTable=[], SenseTable=[])
        ds2 = Dictionary.in_dir(self.tmp_path('d2'))
        ds2.write(EntryTable=[], SenseTable=[])
        db = Database(self.tmp_path('db.sqlite'))
        db.create()
        db.load(ds)
        db.load(ds2)
        ds.tables[0].tableSchema.columns.append(Column(name='newcol', datatype='integer'))
        db.load(ds)
        ds.tables[0].tableSchema.columns[-1].datatype.base = 'string'
        with self.assertRaises(ValueError):
            db.load(ds)

    def test_newcol(self):
        from pycldf.db import Database

        ds = StructureDataset.in_dir(self.tmp_path('d'))
        ds['ValueTable'].tableSchema.columns.append(
            Column(name='col1', datatype='anyURI'))
        ds['ValueTable'].tableSchema.columns.append(
            Column(name='col2', datatype='integer'))
        ds.write(ValueTable=[{
            'ID': '1',
            'Language_ID': 'l',
            'Parameter_ID': 'p',
            'Value': 'v',
            'col2': 5,
            'col1': anyURI().to_python('http://example.org')}])
        db = Database(self.tmp_path('db.sqlite'))
        db.create()
        db.load(ds)
        self.assertEqual(
            db.fetchone("select col1 from valuetable")[0], 'http://example.org')
        self.assertEqual(
            db.fetchone("select col2 from valuetable")[0], 5)
