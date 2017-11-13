from __future__ import unicode_literals

import pytest

from clldutils.path import copy, write_text, Path
from clldutils.csvw.metadata import TableGroup, ForeignKey, URITemplate, Column

from pycldf.terms import term_uri
from pycldf.dataset import (
    Generic, Wordlist, StructureDataset, Dictionary, Dataset, make_column, get_modules)


@pytest.fixture
def ds(tmpdir):
    return Generic.in_dir(str(tmpdir))


@pytest.fixture
def ds_wl(tmpdir):
    return Wordlist.in_dir(str(tmpdir))


@pytest.fixture
def ds_wl_notables(tmpdir):
    return Wordlist.in_dir(str(tmpdir), empty_tables=True)


@pytest.mark.parametrize("col_spec,datatype", [
    ('name', 'string'),
    ({'name': 'num', 'datatype': 'decimal'}, 'decimal'),
    (term_uri('latitude'), 'decimal'),
])
def test_column_basetype(col_spec, datatype):
    assert make_column(col_spec).datatype.base == datatype


def test_make_column():
    assert make_column(term_uri('source')).separator == ';'
    assert make_column(Column('name')).datatype is None
    with pytest.raises(TypeError):
        make_column(5)


def test_primary_table(ds):
    assert ds.primary_table is None


def test_column_access(ds):
    with pytest.raises(KeyError):
        assert ds['']

    ds.add_component('ValueTable')
    assert ds['ValueTable'] == ds['values.csv']

    with pytest.raises(KeyError):
        assert ds['ValueTable', 'colx']

    assert ds['ValueTable', 'Language_ID'] == ds['values.csv', 'languageReference']


def test_foreign_key_creation(ds):
    ds.add_component('ValueTable')
    assert not ds['ValueTable'].tableSchema.foreignKeys
    ds.add_component('LanguageTable')
    assert len(ds['ValueTable'].tableSchema.foreignKeys) == 1
    ds.write(
        ValueTable=[{
            'ID': '1',
            'Language_ID': 'abc',
            'Parameter_ID': 'xyz',
            'Value': '?',
        }],
        LanguageTable=[])
    with pytest.raises(ValueError):
        ds.validate()
    ds.write(
        ValueTable=[{
            'ID': '1',
            'Language_ID': 'abc',
            'Parameter_ID': 'xyz',
            'Value': '?',
        }],
        LanguageTable=[{'ID': 'abc', 'Name': 'language'}])
    ds.validate()


def test_foreign_key_creation_two_fks_to_new_comp(ds):
    ds.add_component('BorrowingTable')
    ds.add_component('FormTable')
    assert len(ds['BorrowingTable'].tableSchema.foreignKeys) == 2


def test_foreign_key_creation_two_fks_from_new_comp(ds):
    ds.add_component('FormTable')
    ds.add_component('BorrowingTable')
    assert len(ds['BorrowingTable'].tableSchema.foreignKeys) == 2
    ds.write(
        FormTable=[{
            'ID': '1',
            'Language_ID': 'abc',
            'Parameter_ID': 'xyz',
            'Form': 'form',
        }],
        BorrowingTable=[{
            'ID': 'abc',
            'Form_ID_Target': '1'}])
    ds.validate()


def test_add_table(tmpdir, ds):
    ds.add_table('stuff.csv', term_uri('id'), 'col1')
    ds.write(fname=str(tmpdir/ 't.json'), **{'stuff.csv': [{'ID': '.ab'}]})
    with pytest.raises(ValueError):
        ds.validate()
    ds['stuff.csv', 'ID'].name = 'nid'
    with pytest.raises(ValueError):
        ds.add_columns('stuff.csv', term_uri('id'))
    with pytest.raises(ValueError):
        ds.add_columns('stuff.csv', 'col1')


def test_add_foreign_key(ds):
    ds.add_table('primary.csv', term_uri('id'), 'col1')
    ds.add_table('foreign.csv', 'fk_id')
    ds.write(**{'primary.csv': [{'ID': 'ab'}], 'foreign.csv': [{'fk_id': 'xy'}]})
    ds.validate()

    ds.add_foreign_key('foreign.csv', 'fk_id', 'primary.csv')
    ds.write(**{'primary.csv': [{'ID': 'ab'}], 'foreign.csv': [{'fk_id': 'xy'}]})
    with pytest.raises(ValueError):
        ds.validate()

    ds.add_foreign_key('foreign.csv', 'fk_id', 'primary.csv', 'ID')
    ds.write(**{'primary.csv': [{'ID': 'ab'}], 'foreign.csv': [{'fk_id': 'xy'}]})
    with pytest.raises(ValueError):
        ds.validate()


def test_in_dir(ds_wl):
    assert len(ds_wl.tables) == 1


def test_in_dir_empty(ds_wl_notables):
    assert len(ds_wl_notables.tables) == 0


def test_cognates(ds_wl):
    ds_wl['FormTable', 'Segments'].separator = None
    ds_wl.write(
        FormTable=[
            {'ID': '1',
             'Value': 'form',
             'Form': 'abcdefg',
             'Segments': 'a bc d e f',
             'Language_ID': 'l',
             'Parameter_ID': 'p'}
        ],
    )
    assert ' '.join(ds_wl.get_soundsequence(list(ds_wl['FormTable'])[0])) == 'a bc d e f'


def test_partial_cognates(ds_wl):
    ds_wl['FormTable'].get_column('Segments').separator = '+'
    ds_wl.add_component('PartialCognateTable')
    ds_wl.write(
        FormTable=[
            {'ID': '1',
             'Value': 'form',
             'Form': 'abcdefg',
             'Segments': ['a bc', 'd e f', 'g'],
             'Language_ID': 'l',
             'Parameter_ID': 'p'}
        ],
        PartialCognateTable=[
            {
                'ID': '1',
                'Form_ID': '1',
                'Cognateset_ID': '1',
                'Slice': ['1:3'],
            }
        ],
    )
    assert ' '.join(ds_wl.get_soundsequence(list(ds_wl['FormTable'])[0])) == 'a bc d e f g'
    assert ' '.join(ds_wl.get_subsequence(list(ds_wl['PartialCognateTable'])[0])) == 'd e f g'


def _make_tg(tmpdir, *tables):
    tg = TableGroup.fromvalue({'tables': list(tables)})
    tg._fname = Path(str(tmpdir / 'md.json'))  # FIXME: clldutils dependency
    return tg


def test_add_component(ds_wl):
    ds_wl['FormTable'].tableSchema.foreignKeys.append(ForeignKey.fromdict({
        'columnReference': 'Language_ID',
        'reference': {'resource': 'languages.csv', 'columnReference': 'ID'}}))
    ds_wl.add_component('LanguageTable')
    with pytest.raises(ValueError):
        ds_wl.add_component('LanguageTable')
    ds_wl.add_component('ParameterTable', {'name': 'url', 'datatype': 'anyURI'})

    ds_wl.write(
        FormTable=[
            {'ID': '1', 'Form': 'form', 'Language_ID': 'l', 'Parameter_ID': 'p'}],
        LanguageTable=[{'ID': 'l'}],
        ParameterTable=[{'ID': 'p'}])
    ds_wl.validate()

    ds_wl.write(
        FormTable=[
            {'ID': '1', 'Value': 'form', 'Language_ID': 'l', 'Parameter_ID': 'x'}],
        LanguageTable=[{'ID': 'l'}],
        ParameterTable=[{'ID': 'p'}])
    with pytest.raises(ValueError):
        ds_wl.validate()


def test_modules(tmpdir):
    ds = Dataset(_make_tg(tmpdir))
    assert ds.primary_table is None
    ds = Dataset(_make_tg(tmpdir, {"url": "data.csv"}))
    assert ds.primary_table is None
    ds = Dataset(_make_tg(tmpdir, {
        "url": "data.csv",
        "dc:conformsTo": "http://cldf.clld.org/v1.0/terms.rdf#ValueTable"}))
    assert ds.primary_table == 'ValueTable'
    assert Wordlist.in_dir(str(tmpdir)).primary_table
    assert Dictionary.in_dir(str(tmpdir)).primary_table
    assert StructureDataset.in_dir(str(tmpdir)).primary_table


def test_Dataset_from_scratch(tmpdir, data):
    # An unknown file name cannot be used with Dataset.from_data:
    copy(str(data / 'ds1.csv'), str(tmpdir / 'xyz.csv'))
    with pytest.raises(ValueError):
        Dataset.from_data(str(tmpdir / 'xyz.csv'))

    # Known file name, but non-standard column name:
    write_text(str(tmpdir / 'values.csv'), "IDX,Language_ID,Parameter_ID,Value\n1,1,1,1")
    with pytest.raises(ValueError):
        ds = Dataset.from_data(str(tmpdir / 'values.csv'))

    # A known file name will determine the CLDF module of the dataset:
    copy(str(data /'ds1.csv'), str(tmpdir / 'values.csv'))
    ds = Dataset.from_data(str(tmpdir / 'values.csv'))
    assert ds.module == 'StructureDataset'

    assert len(list(ds['ValueTable'])) == 2
    ds.validate()
    ds['ValueTable'].write(2 * list(ds['ValueTable']))
    with pytest.raises(ValueError):
        ds.validate()
    md = ds.write_metadata()
    Dataset.from_metadata(md)
    repr(ds)
    del ds._tg.common_props['dc:conformsTo']
    Dataset.from_metadata(ds.write_metadata())
    assert len(ds.stats()) == 1


def test_Dataset_validate(tmpdir):
    ds = StructureDataset.in_dir(str(tmpdir / 'new'))
    ds.write(ValueTable=[])
    ds.validate()
    ds['ValueTable'].tableSchema.columns = []
    with pytest.raises(ValueError):
        ds.validate()
    ds._tg.tables = []
    with pytest.raises(ValueError):
        ds.validate()

    ds = StructureDataset.in_dir(str(tmpdir / 'new'))
    ds.add_component('LanguageTable')
    ds.write(ValueTable=[])
    ds['LanguageTable'].common_props['dc:conformsTo'] = 'http://cldf.clld.org/404'
    with pytest.raises(ValueError):
        ds.validate()

    ds = StructureDataset.in_dir(str(tmpdir / 'new'))
    ds['ValueTable'].get_column('Source').propertyUrl = URITemplate(
        'http://cldf.clld.org/404')
    ds.write(ValueTable=[])
    with pytest.raises(ValueError):
        ds.validate()


def test_Dataset_write(tmpdir):
    ds = StructureDataset.from_metadata(str(tmpdir))
    ds.write(ValueTable=[])
    assert (tmpdir / 'values.csv').exists()
    ds.validate()
    ds.add_sources("@misc{ky,\ntitle={the title}\n}")
    ds.write(ValueTable=[
        {
            'ID': '1',
            'Language_ID': 'abcd1234',
            'Parameter_ID': 'f1',
            'Value': 'yes',
            'Source': ['key[1-20]', 'ky'],
        }])
    with pytest.raises(ValueError):
        ds.validate()
    ds.sources.add("@misc{key,\ntitle={the title}\n}")
    ds.write(ValueTable=[
        {
            'ID': '1',
            'Language_ID': 'abcd1234',
            'Parameter_ID': 'f1',
            'Value': 'yes',
            'Source': ['key[1-20]'],
        }])
    ds.validate()
    ds.add_component('ExampleTable')
    ds.write(
        ValueTable=[
            {
                'ID': '1',
                'Language_ID': 'abcd1234',
                'Parameter_ID': 'f1',
                'Value': 'yes',
                'Source': ['key[1-20]'],
            }],
        ExampleTable=[
            {
                'ID': '1',
                'Language_ID': 'abcd1234',
                'Primary': 'si',
                'Translation': 'yes',
                'Analyzed': ['morph1', 'morph2', 'morph3'],
                'Gloss': ['gl1', 'gl2'],
            }])
    with pytest.raises(ValueError):
        ds.validate()
    ds['ExampleTable'].write([
        {
            'ID': '1',
            'Language_ID': 'abcd1234',
            'Primary': 'si',
            'Translation': 'yes',
            'Analyzed': ['morph1', 'morph2', 'morph3'],
            'Gloss': ['gl1', 'gl2', 'gl3'],
        }])
    ds.validate()


def test_validators(tmpdir, mocker, data):
    copy(str(data / 'invalid.csv'), str(tmpdir / 'values.csv'))
    ds = Dataset.from_data(str(tmpdir / 'values.csv'))

    with pytest.raises(ValueError):
        ds.validate()

    log = mocker.Mock()
    ds.validate(log=log)
    assert log.warn.call_count == 2

    for col in ds._tg.tables[0].tableSchema.columns:
        if col.name == 'Language_ID':
            col.propertyUrl.uri = 'http://cldf.clld.org/v1.0/terms.rdf#glottocode'

    log = mocker.Mock()
    ds.validate(log=log)
    assert log.warn.call_count == 4


def test_get_modules():
    assert not get_modules()[0].match(5)
