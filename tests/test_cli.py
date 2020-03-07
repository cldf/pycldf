import shutil
import logging
import warnings

import pytest

from clldutils.path import copy

from pycldf.__main__ import main


def test_help(capsys):
    main([])
    out, _ = capsys.readouterr()
    assert 'usage' in out


def test_stats(tmpdir):
    with pytest.raises(SystemExit):
        main(['stats'])

    with pytest.raises(SystemExit):
        main(['stats', str(tmpdir / 'new')])


def test_check(data, glottolog_repos, caplog, tmpdir):
    res = main(
            ['check', str(data / 'dataset_for_check' / 'metadata.json'), str(glottolog_repos)],
            log=logging.getLogger(__name__))
    assert res == 2
    assert len(caplog.records) == 2

    assert main(
        ['check', str(data / 'ds1.csv-metadata.json'), str(glottolog_repos)],
        log=logging.getLogger(__name__)) == 0

    shutil.copy(str(data / 'dataset_for_check' / 'metadata.json'), str(tmpdir))
    tmpdir.join('languages.csv').write_text('ID,Glottocode', encoding='utf8')
    res = main(
        ['check', str(tmpdir.join('metadata.json')), str(glottolog_repos)],
        log=logging.getLogger(__name__))
    assert res == 2
    assert 'No languages' in caplog.records[-1].message


def test_all(capsys, tmpdir, mocker, data):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        md = str(tmpdir / 'md.json')
        copy(str(data / 'ds1.csv-metadata.json'), md)
        copy(str(data / 'ds1.bib'), str(tmpdir / 'ds1.bib'))
        copy(str(data / 'ds1.csv'), str(tmpdir / 'ds1.csv'))
        pdata = str(tmpdir / 'values.csv')
        copy(str(data / 'ds1.csv'), pdata)

        main(['validate', md])
        out, err = capsys.readouterr()
        assert not out

        main(['stats', pdata])
        out, err = capsys.readouterr()
        assert 'StructureDataset' in out

        main(['stats', md])

        with pytest.raises(SystemExit):
            main(['createdb', md])

        log = mocker.MagicMock()
        main(['createdb', md, str(tmpdir / 'test.sqlite')], log=log)
        assert log.info.called
        main(['dumpdb', md, str(tmpdir / 'test.sqlite')], log=log)

        uc = [
            w_ for w_ in w
            if issubclass(w_.category, UserWarning) and
               str(w_.message).startswith('Unspecified column')]
        assert uc

    with pytest.raises(SystemExit):
        main(['createdb', md, str(tmpdir / 'test.sqlite')], log=log)
