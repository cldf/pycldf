import decimal
import sqlite3
import warnings

import pytest

from pycldf.dataset import Dataset, Generic
from pycldf.db import Database, translate, TableTranslation


@pytest.fixture
def md(tmp_path):
    return tmp_path / 'metadata.json'


def test_db_geocoords():
    item = dict(cldf_latitude=decimal.Decimal(3.123456))
    assert pytest.approx(
        Database.round_geocoordinates(item)['cldf_latitude']) == decimal.Decimal(3.1235)


def test_db_write(tmp_path, data):
    ds = Dataset.from_metadata(data / 'ds1.csv-metadata.json')
    db = Database(ds, fname=tmp_path / 'db.sqlite')
    db.write_from_tg()
    assert len(db.query("select * from ValueTable where cldf_parameterReference = 'fid1'")) == 1
    assert len(db.query('select * from SourceTable')) == 3
    assert len(db.query(
        "select valuetable_cldf_id from ValueTable_SourceTable where context = '2-5'")) == 1

    assert db.read()['ValueTable'][0]['cldf_source'] == ['80086', 'meier2015[2-5]']
    db.to_cldf(tmp_path / 'cldf')
    assert tmp_path.joinpath('cldf', 'ds1.bib').exists()
    assert '80086;meier2015[2-5]' in tmp_path.joinpath('cldf', 'ds1.csv').read_text(encoding='utf8')

    with pytest.raises(ValueError):
        db.write_from_tg()

    with pytest.raises(NotImplementedError):
        db.write_from_tg(_exists_ok=True)

    db.write_from_tg(_force=True)


def test_db_write_extra_tables(md):
    ds = Generic.in_dir(md.parent)
    ds.add_table('extra.csv', 'ID', 'Name', {'name': 'x', 'separator': '#'})
    ds.write(md, **{'extra.csv': [dict(ID=1, Name='Name', x=['a', 'b', 'c'])]})

    db = Database(ds, fname=md.parent / 'db.sqlite')
    db.write_from_tg()
    rows = db.query("""select x from "extra.csv" """)
    assert len(rows) == 1
    assert rows[0][0] == 'a#b#c'
    assert db.split_value('extra.csv', 'x', rows[0][0]) == ['a', 'b', 'c']


def test_db_write_extra_columns(md):
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        ds = Generic.in_dir(md.parent)
        t = ds.add_table('extra.csv', 'ID', 'Name')
        ds.write(md, **{'extra.csv': [dict(ID=1, Name='Name')]})
        t.tableSchema.columns = [c for c in t.tableSchema.columns if c.name != 'Name']
        ds.write_metadata(md)

        db = Database(ds, fname=md.parent / 'db.sqlite')
        assert len(db.dataset['extra.csv'].tableSchema.columns) == 1
        db.write_from_tg()
        assert len(db.query("""select * from "extra.csv" """)[0]) == 1


def test_db_write_tables_with_fks(md):
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


def test_db_translations_for_association_table(data, tmp_path):
    dbpath = tmp_path / 'db.sqlite'
    dsdir = data / 'dataset_with_listvalued_foreign_keys_to_component'
    ds = Dataset.from_metadata(dsdir / 'metadata.json')
    db = Database(ds, fname=dbpath)
    db.write_from_tg()
    #assert len(db.query('select * from "forms.csv_concepts.csv"')) == 2
    q = db.query('select ParameterTable_cldf_id from FormTable_ParameterTable')
    assert {r[0] for r in q} == {'c1', 'c2'}
    q = db.query('select "custom.csv_cldf_id" from "FormTable_custom.csv"')
    assert {r[0] for r in q} == {'1'}

    db.to_cldf(dest=dbpath.parent)
    assert dbpath.parent.joinpath('forms.csv').read_text('utf8').strip() == \
           dsdir.joinpath('forms.csv').read_text('utf8').strip()


@pytest.fixture
def translations():
    return {
        'forms.csv': TableTranslation(columns={'pk': 'id'}),
        'parameters.csv': TableTranslation(name='PTable', columns={'pk': 'id'}),
    }


@pytest.mark.parametrize(
    'table,col,expected',
    [
        ('forms.csv', None, 'forms.csv'),  # Table has no name in translations!
        ('forms.csv', 'pk', 'id'),
        ('forms.csv_parameters.csv', None, 'forms.csv_PTable'),
        ('forms.csv_parameters.csv', 'parameters.csv_pk', 'PTable_id'),
        ('forms.csv_parameters.csv', 'forms.csv_pk', 'forms.csv_id'),
    ]
)
def test_translate(translations, table, col, expected):
    assert translate(translations, table, col) == expected


def test_TableTranslation():
    t1 = TableTranslation()
    t1.columns['a'] = 'b'
    assert t1.columns
    t2 = TableTranslation()
    assert not t2.columns
