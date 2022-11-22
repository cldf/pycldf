import pathlib
import sys
import shutil
import logging
import sqlite3
import warnings

import pytest

from pycldf.__main__ import main


def test_help(capsys):
    main([])
    out, _ = capsys.readouterr()
    assert 'usage' in out


def test_markdown(capsys, data, tmp_path):
    main(['markdown', str(data / 'ds1.csv-metadata.json')])
    out, _ = capsys.readouterr()
    assert 'a cldf dataset' in out

    out = tmp_path / 'test.md'
    main([
        'markdown',
        str(data / 'dataset_with_listvalued_foreign_keys_to_component' / 'metadata.json'),
        '--out', str(out)])
    assert 'References' in out.read_text(encoding='utf8')


def test_stats(tmp_path):
    with pytest.raises(SystemExit):
        main(['stats'])

    with pytest.raises(SystemExit):
        main(['stats', str(tmp_path / 'new')])


def test_check(data, glottolog_repos, concepticon_repos, caplog, tmp_path):
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

    shutil.copy(str(data / 'dataset_for_check' / 'metadata.json'), tmp_path)
    shutil.copy(str(data / 'dataset_for_check' / 'parameters.csv'), tmp_path)
    tmp_path.joinpath('languages.csv').write_text(
        'ID,Glottocode,Latitude,ISO,ma,lon', encoding='utf8')
    res = main(['check', str(tmp_path.joinpath('metadata.json'))], log=logging.getLogger(__name__))
    assert res == 2
    assert 'Empty ' in caplog.records[-1].message


def test_downloadmedia(tmp_path, data):
    from pycldf import Dataset
    from pycldf.media import MediaTable

    md = data / 'dataset_with_media' / 'metadata.json'
    main(['downloadmedia', str(md), str(tmp_path), "Name=x"])
    assert len(list(tmp_path.glob('*'))) == 1

    main(['downloadmedia', str(md), str(tmp_path)])
    files = list(MediaTable(Dataset.from_metadata(md)))
    assert files[0].read(tmp_path) == 'Hello, World!'
    assert files[1].read(tmp_path) == 'äöü'


def test_validate(tmp_path, caplog):
    tmp_path.joinpath('md.json').write_text("""{
  "@context": ["http://www.w3.org/ns/csvw", {"@language": "en"}],
  "dc:conformsTo": "http://cldf.clld.org/v1.0/terms.rdf#StructureDataset",
  "tables": []
}""", encoding='utf8')
    # A StructureDataset must speficy a ValueTable!
    assert main(['validate', str(tmp_path / 'md.json')], log=logging.getLogger(__name__)) == 1
    assert all(
        w in caplog.records[-1].message for w in ['StructureDataset', 'requires', 'ValueTable'])


def test_all(capsys, tmp_path, mocker, data):
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        md = tmp_path / 'md.json'
        shutil.copy(data / 'ds1.csv-metadata.json', md)
        shutil.copy(data / 'ds1.bib', tmp_path / 'ds1.bib')
        shutil.copy(data / 'ds1.csv', tmp_path / 'ds1.csv')
        pdata = tmp_path / 'values.csv'
        shutil.copy(data / 'ds1.csv', pdata)

        assert main(['validate', str(md)]) == 0
        out, err = capsys.readouterr()
        assert not out

        main(['stats', str(pdata)])
        out, err = capsys.readouterr()
        assert 'StructureDataset' in out

        main(['stats', str(md)])

        with pytest.raises(SystemExit):
            main(['createdb', str(md)])

        log = mocker.MagicMock()
        main(['createdb', str(md), str(tmp_path / 'test.sqlite')], log=log)
        assert log.info.called
        main(['dumpdb', str(md), str(tmp_path / 'test.sqlite')], log=log)

        uc = [
            w_ for w_ in w
            if issubclass(w_.category, UserWarning) and
               str(w_.message).startswith('Unspecified column')]
        assert uc

    with pytest.raises(SystemExit):
        main(['createdb', str(md), str(tmp_path / 'test.sqlite')], log=log)


def test_createdb_locator(data, tmp_path):
    db = tmp_path / 'db.sqlite'
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        main(['createdb', str(data) + '#rdf:ID=dswm', str(db)])
    assert db.exists()
    conn = sqlite3.connect(str(db))
    cu = conn.cursor()
    cu.execute('select count(*) from mediatable')
    assert cu.fetchone()[0] > 0
