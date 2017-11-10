# coding: utf8
from __future__ import unicode_literals, print_function, division

import pytest
from mock import Mock
from clldutils.csvw.metadata import TableGroup, ForeignKey, URITemplate, Column
from clldutils.path import copy, write_text

from pycldf.terms import term_uri
from pycldf.dataset import (
    Generic, Wordlist, StructureDataset, Dictionary, Dataset, make_column, get_modules)


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


def _make_ds(d, cls=Generic, **kw):
    return cls.in_dir(d, **kw)


def test_primary_table(tmp_dir):
    assert _make_ds(tmp_dir).primary_table is None


def test_column_access(tmp_dir):
    ds = _make_ds(tmp_dir)
    with pytest.raises(KeyError):
        assert ds['']

    ds.add_component('ValueTable')
    assert ds['ValueTable'] == ds['values.csv']

    with pytest.raises(KeyError):
        assert ds['ValueTable', 'colx']

    assert ds['ValueTable', 'Language_ID'] == ds['values.csv', 'languageReference']


def test_foreign_key_creation(tmp_dir):
    ds = _make_ds(tmp_dir)
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


def test_foreign_key_creation_two_fks_to_new_comp(tmp_dir):
    ds = _make_ds(tmp_dir)
    ds.add_component('BorrowingTable')
    ds.add_component('FormTable')
    assert len(ds['BorrowingTable'].tableSchema.foreignKeys) == 2


def test_foreign_key_creation_two_fks_from_new_comp(tmp_dir):
    ds = _make_ds(tmp_dir)
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


def test_add_table(tmp_dir):
    ds = _make_ds(tmp_dir)
    ds.add_table('stuff.csv', term_uri('id'), 'col1')
    ds.write(fname=tmp_dir / 't.json', **{'stuff.csv': [{'ID': '.ab'}]})
    with pytest.raises(ValueError):
        ds.validate()
    ds['stuff.csv', 'ID'].name = 'nid'
    with pytest.raises(ValueError):
        ds.add_columns('stuff.csv', term_uri('id'))
    with pytest.raises(ValueError):
        ds.add_columns('stuff.csv', 'col1')


def test_add_foreign_key(tmp_dir):
    ds = _make_ds(tmp_dir)
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


def test_in_dir(tmp_dir):
    ds = _make_ds(tmp_dir, Wordlist)
    assert len(ds.tables) == 1
    ds = _make_ds(tmp_dir, Wordlist, empty_tables=True)
    assert len(ds.tables) == 0


def test_cognates(tmp_dir):
    ds = _make_ds(tmp_dir, Wordlist)
    ds['FormTable', 'Segments'].separator = None
    ds.write(
        FormTable=[
            {'ID': '1',
             'Value': 'form',
             'Form': 'abcdefg',
             'Segments': 'a bc d e f',
             'Language_ID': 'l',
             'Parameter_ID': 'p'}
        ],
    )
    assert ' '.join(ds.get_soundsequence(list(ds['FormTable'])[0])) == 'a bc d e f'


def test_partial_cognates(tmp_dir):
    ds = _make_ds(tmp_dir, Wordlist)
    ds['FormTable'].get_column('Segments').separator = '+'
    ds.add_component('PartialCognateTable')
    ds.write(
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
    assert ' '.join(ds.get_soundsequence(list(ds['FormTable'])[0])) == 'a bc d e f g'
    assert ' '.join(ds.get_subsequence(list(ds['PartialCognateTable'])[0])) == 'd e f g'


def _make_tg(tmp_dir, *tables):
    tg = TableGroup.fromvalue({'tables': list(tables)})
    tg._fname = tmp_dir / 'md.json'
    return tg


def test_add_component(tmp_dir):
    ds = _make_ds(tmp_dir, Wordlist)
    ds['FormTable'].tableSchema.foreignKeys.append(ForeignKey.fromdict({
        'columnReference': 'Language_ID',
        'reference': {'resource': 'languages.csv', 'columnReference': 'ID'}}))
    ds.add_component('LanguageTable')
    with pytest.raises(ValueError):
        ds.add_component('LanguageTable')
    ds.add_component('ParameterTable', {'name': 'url', 'datatype': 'anyURI'})

    ds.write(
        FormTable=[
            {'ID': '1', 'Form': 'form', 'Language_ID': 'l', 'Parameter_ID': 'p'}],
        LanguageTable=[{'ID': 'l'}],
        ParameterTable=[{'ID': 'p'}])
    ds.validate()

    ds.write(
        FormTable=[
            {'ID': '1', 'Value': 'form', 'Language_ID': 'l', 'Parameter_ID': 'x'}],
        LanguageTable=[{'ID': 'l'}],
        ParameterTable=[{'ID': 'p'}])
    with pytest.raises(ValueError):
        ds.validate()


def test_modules(tmp_dir):
    ds = Dataset(_make_tg(tmp_dir))
    assert ds.primary_table is None
    ds = Dataset(_make_tg(tmp_dir, {"url": "data.csv"}))
    assert ds.primary_table is None
    ds = Dataset(_make_tg(tmp_dir, {
        "url": "data.csv",
        "dc:conformsTo": "http://cldf.clld.org/v1.0/terms.rdf#ValueTable"}))
    assert ds.primary_table == 'ValueTable'
    assert Wordlist.in_dir(tmp_dir).primary_table
    assert Dictionary.in_dir(tmp_dir).primary_table
    assert StructureDataset.in_dir(tmp_dir).primary_table


def test_Dataset_from_scratch(tmp_dir, data):
    # An unknown file name cannot be used with Dataset.from_data:
    copy(data.joinpath('ds1.csv'), tmp_dir / 'xyz.csv')
    with pytest.raises(ValueError):
        Dataset.from_data(tmp_dir / 'xyz.csv')

    # Known file name, but non-standard column name:
    write_text(tmp_dir / 'values.csv', "IDX,Language_ID,Parameter_ID,Value\n1,1,1,1")
    with pytest.raises(ValueError):
        ds = Dataset.from_data(tmp_dir / 'values.csv')

    # A known file name will determine the CLDF module of the dataset:
    copy(data.joinpath('ds1.csv'), tmp_dir / 'values.csv')
    ds = Dataset.from_data(tmp_dir / 'values.csv')
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


def test_Dataset_validate(tmp_dir):
    ds = StructureDataset.in_dir(tmp_dir / 'new')
    ds.write(ValueTable=[])
    ds.validate()
    ds['ValueTable'].tableSchema.columns = []
    with pytest.raises(ValueError):
        ds.validate()
    ds._tg.tables = []
    with pytest.raises(ValueError):
        ds.validate()

    ds = StructureDataset.in_dir(tmp_dir / 'new')
    ds.add_component('LanguageTable')
    ds.write(ValueTable=[])
    ds['LanguageTable'].common_props['dc:conformsTo'] = 'http://cldf.clld.org/404'
    with pytest.raises(ValueError):
        ds.validate()

    ds = StructureDataset.in_dir(tmp_dir / 'new')
    ds['ValueTable'].get_column('Source').propertyUrl = URITemplate(
        'http://cldf.clld.org/404')
    ds.write(ValueTable=[])
    with pytest.raises(ValueError):
        ds.validate()


def test_Dataset_write(tmp_dir):
    ds = StructureDataset.from_metadata(tmp_dir)
    ds.write(ValueTable=[])
    assert (tmp_dir / 'values.csv').exists()
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


def test_validators(data, tmp_dir):
    copy(data.joinpath('invalid.csv'), tmp_dir / 'values.csv')
    ds = Dataset.from_data(tmp_dir / 'values.csv')

    with pytest.raises(ValueError):
        ds.validate()

    log = Mock()
    ds.validate(log=log)
    assert log.warn.call_count == 2

    for col in ds._tg.tables[0].tableSchema.columns:
        if col.name == 'Language_ID':
            col.propertyUrl.uri = 'http://cldf.clld.org/v1.0/terms.rdf#glottocode'

    log = Mock()
    ds.validate(log=log)
    assert log.warn.call_count == 4


def test_get_modules():
    assert not get_modules()[0].match(5)
