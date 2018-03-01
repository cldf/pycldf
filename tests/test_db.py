from __future__ import unicode_literals

import pytest

from sqlite3 import IntegrityError

from csvw.metadata import Column
from csvw.datatypes import anyURI

from pycldf.dataset import Dataset, Dictionary, StructureDataset


@pytest.fixture
def db(tmpdir):
    from pycldf.db import Database

    return Database(str(tmpdir / 'db.sqlite'))


def test_db(data, db):
    ds = Dataset.from_metadata(str(data / 'ds1.csv-metadata.json'))
    db.create()
    db.load(ds)
    assert len(db.fetchall("SELECT name FROM dataset")) == 1
    with pytest.raises(IntegrityError):
        db.load(ds)
    db.delete(db.fetchone("SELECT ID FROM dataset")[0])
    db.load(ds)
    db.drop()


def test_create(db):
    db.create()
    with pytest.raises(ValueError):
        db.create()
    db.create(force=True)


def test_update(tmpdir, db):
    ds = Dictionary.in_dir(str(tmpdir / 'd1'))
    ds.write(EntryTable=[], SenseTable=[])
    ds2 = Dictionary.in_dir(str(tmpdir / 'd2'))
    ds2.write(EntryTable=[], SenseTable=[])
    db.create()
    db.load(ds)
    db.load(ds2)
    ds.tables[0].tableSchema.columns.append(Column(name='newcol', datatype='integer'))
    db.load(ds)
    ds.tables[0].tableSchema.columns[-1].datatype.base = 'string'
    with pytest.raises(ValueError):
        db.load(ds)


def test_newcol(tmpdir, db):
    ds = StructureDataset.in_dir(str(tmpdir / 'd'))

    # We rename the ID column of the ValueTable. Note that the propertyUrl
    # remains the same:
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
    db.create()
    with pytest.raises(IntegrityError):  # A missing source is referenced!
        db.load(ds)
    ds.add_sources("@misc{meier2015,\ntitle={title}\n}")
    db.load(ds)
    assert db.fetchone("""\
select
  s.title
from
  SourceTable as s, ValueSource as vs, ValueTable as v
where
  s.ID = vs.Source_ID and vs.Value_ID = v.id and v.id = 1""")[0] == 'title'
    assert db.fetchone("select col1 from valuetable")[0] == 'http://example.org'
    assert db.fetchone("select col2 from valuetable")[0] == 5
