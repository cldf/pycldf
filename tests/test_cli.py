import sys
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


def test_markdown(capsys, data):
    main(['markdown', str(data / 'ds1.csv-metadata.json')])
    out, _ = capsys.readouterr()
    assert 'a cldf dataset' in out

    main([
        'markdown',
        str(data / 'dataset_with_listvalued_foreign_keys_to_component' / 'metadata.json')])
    out, _ = capsys.readouterr()
    assert 'References' in out


def test_stats(tmpdir):
    with pytest.raises(SystemExit):
        main(['stats'])

    with pytest.raises(SystemExit):
        main(['stats', str(tmpdir / 'new')])


def test_check(data, glottolog_repos, concepticon_repos, caplog, tmpdir):
    res = main(
            [
                'check',
                '--iso-codes',
                str(data / 'iso_codes.tab'),
                str(data / 'dataset_for_check' / 'metadata.json'),
                '--concepticon',
                str(concepticon_repos),
                '--glottolog',
                str(glottolog_repos)],
            log=logging.getLogger(__name__))
    if sys.version_info >= (3, 6):
        assert res == 2
        assert len(caplog.records) == 7

    assert main(
        ['check', str(data / 'ds1.csv-metadata.json')],
        log=logging.getLogger(__name__)) == 0

    shutil.copy(str(data / 'dataset_for_check' / 'metadata.json'), str(tmpdir))
    shutil.copy(str(data / 'dataset_for_check' / 'parameters.csv'), str(tmpdir))
    tmpdir.join('languages.csv').write_text('ID,Glottocode,Latitude,ISO,ma,lon', encoding='utf8')
    res = main(['check', str(tmpdir.join('metadata.json'))], log=logging.getLogger(__name__))
    assert res == 2
    assert 'Empty ' in caplog.records[-1].message


def test_validate(tmpdir, caplog):
    tmpdir.join('md.json').write_text("""{
  "@context": ["http://www.w3.org/ns/csvw", {"@language": "en"}],
  "dc:conformsTo": "http://cldf.clld.org/v1.0/terms.rdf#StructureDataset",
  "tables": []
}""", encoding='utf8')
    # A StructureDataset must speficy a ValueTable!
    assert main(['validate', str(tmpdir.join('md.json'))], log=logging.getLogger(__name__)) == 1
    assert all(
        w in caplog.records[-1].message for w in ['StructureDataset', 'requires', 'ValueTable'])


def test_all(capsys, tmpdir, mocker, data):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        md = str(tmpdir / 'md.json')
        copy(str(data / 'ds1.csv-metadata.json'), md)
        copy(str(data / 'ds1.bib'), str(tmpdir / 'ds1.bib'))
        copy(str(data / 'ds1.csv'), str(tmpdir / 'ds1.csv'))
        pdata = str(tmpdir / 'values.csv')
        copy(str(data / 'ds1.csv'), pdata)

        assert main(['validate', md]) == 0
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
