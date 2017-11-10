# coding: utf8
from __future__ import unicode_literals, print_function, division

from mock import MagicMock
import pytest
from clldutils.clilib import ParserError
from clldutils.path import copy

from pycldf.cli import validate, stats, createdb


def test_stats(tmp_dir):
    with pytest.raises(ParserError):
        stats(MagicMock(args=MagicMock()))

    with pytest.raises(ParserError):
        stats(MagicMock(args=[tmp_dir.joinpath('new').as_posix()]))


def test_all(capsys, data, tmp_dir):
    md = tmp_dir / 'md.json'
    copy(data.joinpath('ds1.csv-metadata.json'), md)
    copy(data.joinpath('ds1.bib'), tmp_dir / 'ds1.bib')
    copy(data.joinpath('ds1.csv'), tmp_dir / 'ds1.csv')
    pdata = tmp_dir / 'values.csv'
    copy(data.joinpath('ds1.csv'), pdata)

    validate(MagicMock(args=[md.as_posix()]))
    out, err = capsys.readouterr()
    assert not out

    stats(MagicMock(args=[pdata.as_posix()]))
    out, err = capsys.readouterr()
    assert 'StructureDataset' in out

    stats(MagicMock(args=[md.as_posix()]))

    with pytest.raises(ParserError):
        createdb(MagicMock(args=[md.as_posix()]))

    log = MagicMock()
    createdb(MagicMock(log=log, args=[md.as_posix(), tmp_dir / 'test.sqlite']))
    assert log.info.called
