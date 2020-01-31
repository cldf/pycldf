import decimal

import pytest

from pycldf.dataset import Dataset
from pycldf.db import Database


def test_db_geocoords():
    item = dict(cldf_latitude=decimal.Decimal(3.123456))
    assert pytest.approx(
        Database.round_geocoordinates(item)['cldf_latitude'],
        decimal.Decimal(3.1235))


def test_db_write(tmpdir, data):
    #import shutil
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
