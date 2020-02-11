import decimal
import pathlib
import sqlite3

import pytest

from pycldf.dataset import Dataset, Generic
from pycldf.db import Database


def test_db_geocoords():
    item = dict(cldf_latitude=decimal.Decimal(3.123456))
    assert pytest.approx(
        Database.round_geocoordinates(item)['cldf_latitude'],
        decimal.Decimal(3.1235))


def test_db_write(tmpdir, data):
    ds = Dataset.from_metadata(data / 'ds1.csv-metadata.json')
    db = Database(ds, fname=str(tmpdir.join('db.sqlite')))
    db.write_from_tg()
    #shutil.copy(str(tmpdir.join('db.sqlite')), 'db.sqlite')
    assert len(db.query("select * from ValueTable where cldf_parameterReference = 'fid1'")) == 1
    assert len(db.query('select * from SourceTable')) == 3
    assert len(db.query(
        "select valuetable_cldf_id from ValueTable_SourceTable where context = '2-5'")) == 1

    assert db.read()['ValueTable'][0]['cldf_source'] == ['80086', 'meier2015[2-5]']
    db.to_cldf(str(tmpdir.join('cldf')))
    assert tmpdir.join('cldf', 'ds1.bib').check()
    assert '80086;meier2015[2-5]' in tmpdir.join('cldf', 'ds1.csv').read_text('utf8')

    with pytest.raises(ValueError):
        db.write_from_tg()

    with pytest.raises(NotImplementedError):
        db.write_from_tg(_exists_ok=True)

    db.write_from_tg(_force=True)


def test_db_write_extra_tables(tmpdir):
    md = pathlib.Path(str(tmpdir)) / 'metadata.json'
    ds = Generic.in_dir(md.parent)
    ds.add_table('extra.csv', 'ID', 'Name')
    ds.write(md, **{'extra.csv': [dict(ID=1, Name='Name')]})

    db = Database(ds, fname=md.parent / 'db.sqlite')
    db.write_from_tg()
    assert len(db.query("""select * from "extra.csv" """)) == 1


def test_db_write_tables_with_fks(tmpdir, mocker):
    md = pathlib.Path(str(tmpdir)) / 'metadata.json'

    ds = Generic.in_dir(md.parent)
    t1 = ds.add_table('t1.csv', 'ID', 'Name')
    t2 = ds.add_table('t2.csv', 'ID', {'name': 'T1_ID', 'separator': ' '})
    t2.add_foreign_key('T1_ID', 't1.csv', 'ID')
    ds.write(md, **{
        't1.csv': [dict(ID='1', Name='Name')],
        't2.csv': [dict(ID='1', T1_ID=['1'])],
    })
    with pytest.raises(AssertionError):
        _ = Database(ds, fname=md.parent / 'db.sqlite')

    # Primary keys must be inferred ...
    db = Database(ds, fname=md.parent / 'db.sqlite', infer_primary_keys=True)
    db.write_from_tg()

    # ... or declared explicitly:
    t2.tableSchema.primaryKey = ['ID']
    db = Database(ds, fname=md.parent / 'db.sqlite')
    with pytest.raises(sqlite3.OperationalError):
        db.write_from_tg(_force=True)

    t1.tableSchema.primaryKey = ['ID']
    db = Database(ds, fname=md.parent / 'db.sqlite')
    db.write_from_tg(_force=True)

    ds = Generic.in_dir(md.parent)
    ds.add_table('t1.csv', 'ID', 'Name', primaryKey='ID')
    table = ds.add_table('t2.csv', 'ID', {'name': 'T1_ID', 'separator': ' '}, primaryKey='ID')
    table.add_foreign_key('T1_ID', 't1.csv', 'ID')
    ds.write(md, **{
        't1.csv': [dict(ID='1', Name='Name')],
        't2.csv': [dict(ID=1, T1_ID=['1'])],
    })
    db = Database(ds, fname=md.parent / 'db.sqlite')
    db.write_from_tg(_force=True)
