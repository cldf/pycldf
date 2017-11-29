from __future__ import unicode_literals

import pytest

from clldutils.path import copy
from clldutils.clilib import ParserError

from pycldf.__main__ import validate, stats, createdb


def test_stats(tmpdir, mocker):
    with pytest.raises(ParserError):
        stats(mocker.MagicMock(args=mocker.MagicMock()))

    with pytest.raises(ParserError):
        stats(mocker.MagicMock(args=[str(tmpdir / 'new')]))


def test_all(capsys, tmpdir, mocker, data):
    md = str(tmpdir / 'md.json')
    copy(str(data / 'ds1.csv-metadata.json'), md)
    copy(str(data / 'ds1.bib'), str(tmpdir / 'ds1.bib'))
    copy(str(data / 'ds1.csv'), str(tmpdir / 'ds1.csv'))
    pdata = str(tmpdir / 'values.csv')
    copy(str(data / 'ds1.csv'), pdata)

    validate(mocker.MagicMock(args=[md]))
    out, err = capsys.readouterr()
    assert not out

    stats(mocker.MagicMock(args=[pdata]))
    out, err = capsys.readouterr()
    assert 'StructureDataset' in out

    stats(mocker.MagicMock(args=[md]))

    with pytest.raises(ParserError):
        createdb(mocker.MagicMock(args=[md]))

    log = mocker.MagicMock()
    createdb(mocker.MagicMock(log=log, args=[md, str(tmpdir / 'test.sqlite')]))
    assert log.info.called
