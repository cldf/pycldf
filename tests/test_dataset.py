import logging
import warnings
import urllib.parse
from pathlib import Path

import pytest

from csvw.metadata import TableGroup, ForeignKey, URITemplate, Column, Table, Link, Datatype
from clldutils.path import copy

from pycldf.terms import term_uri
from pycldf.dataset import (
    Generic, Wordlist, StructureDataset, Dictionary, ParallelText, Dataset, GitRepository,
    make_column, get_modules, iter_datasets)


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


def test_column_names(ds_wl):
    cn = ds_wl.column_names
    assert cn.values is None
    assert cn.forms
    assert cn.forms.id == 'ID'
    assert cn.forms.cognatesetReference is None

    with pytest.raises(AttributeError):
        cn.unknown_component

    with pytest.raises(AttributeError):
        cn.forms.unknown_property


def test_provenance(ds, tmpdir):
    ds.add_provenance(wasDerivedFrom=[GitRepository('http://example.org'), 'other'])
    assert ds.properties['prov:wasDerivedFrom'][0]['rdf:about'] == 'http://example.org'

    ds.add_provenance(wasDerivedFrom='abc')
    assert len(ds.properties['prov:wasDerivedFrom']) == 3
    
    ds.tablegroup.common_props = {}
    ds.add_provenance(wasDerivedFrom='abc')
    ds.add_provenance(wasDerivedFrom='abc')
    assert len(ds.properties['prov:wasDerivedFrom']) == 1
    ds.add_provenance(wasDerivedFrom='xyz')
    assert len(ds.properties['prov:wasDerivedFrom']) == 2

    ds.tablegroup.common_props = {}
    ds.add_provenance(wasDerivedFrom=GitRepository('http://example.org'))
    assert ds.properties['prov:wasDerivedFrom']['rdf:about'] == 'http://example.org'

    ds.tablegroup.common_props = {}
    ds.add_provenance(wasDerivedFrom=GitRepository('http://example.org', version='v1'))
    assert ds.properties['prov:wasDerivedFrom']['dc:created'] == 'v1'

    ds.tablegroup.common_props = {}
    ds.add_provenance(wasDerivedFrom=GitRepository('http://example.org', clone=str(tmpdir)))
    assert ds.properties['prov:wasDerivedFrom']['dc:created']


def test_primary_table(ds):
    assert ds.primary_table is None


def test_column_access(ds):
    with pytest.raises(KeyError):
        assert ds['']

    ds.add_component('ValueTable', url='datapoints.csv')
    assert ds['ValueTable'] == ds['datapoints.csv']

    with pytest.raises(KeyError):
        assert ds['ValueTable', 'colx']

    assert ds['ValueTable', 'Language_ID'] == ds['datapoints.csv', 'languageReference']


def test_tabletype_none(ds):
    ds.add_table('url', {'name': 'col'})
    ds['url'].common_props['dc:conformsTo'] = None
    assert ds.get_tabletype(ds['url']) is None

    with pytest.raises(ValueError):
        ds.add_table('url')

    # Make sure we can add another table with:
    t = ds.add_component({'url': 'other', 'dc:conformsTo': None})
    assert ds.get_tabletype(t) is None


def test_example_validators(ds, tmpdir):
    ds.add_table(
        'examples',
        {
            'name': 'morphemes',
            'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#analyzedWord',
            'separator': '\t'},
        {
            'name': 'gloss',
            'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#gloss',
            'separator': '\t'},
    )
    ds.write(examples=[{'morphemes': ['a'], 'gloss': ['a', 'b']}])
    with pytest.raises(ValueError) as e:
        ds.validate()
    assert 'number of morphemes' in str(e.value)


def test_duplicate_component(ds, tmpdir):
    # adding a component twice is not possible:
    t = ds.add_component('ValueTable')
    t.url = Link('other.csv')
    with pytest.raises(ValueError):
        ds.add_component('ValueTable')

    # JSON descriptions with duplicate components cannot be read:
    md = tmpdir / 'md.json'
    json = """\
{
    "@context": ["http://www.w3.org/ns/csvw", {"@language": "en"}],
    "dc:conformsTo": "http://cldf.clld.org/v1.0/terms.rdf#StructureDataset",
    "tables": [
        {"url": "values.csv"},
        COMPS 
    ]
}"""
    comp = """
{
    "url": "values.csv",
    "dc:conformsTo": "http://cldf.clld.org/v1.0/terms.rdf#ValueTable",
    "tableSchema": {
        "columns": [
            {
                "name": "ID",
                "propertyUrl": "http://cldf.clld.org/v1.0/terms.rdf#id"
            },
            {
                "name": "Language_ID",
                "propertyUrl": "http://cldf.clld.org/v1.0/terms.rdf#languageReference"
            },
            {
                "name": "Parameter_ID",
                "propertyUrl": "http://cldf.clld.org/v1.0/terms.rdf#parameterReference"
            },
            {
                "name": "Value",
                "propertyUrl": "http://cldf.clld.org/v1.0/terms.rdf#value"
            }
        ]
    }
}"""

    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")

        md.write_text(json.replace('COMPS', comp), encoding='utf8')
        (tmpdir / 'values.csv').write_text(
            "ID,Language_ID,Parameter_ID,Value\n1,1,1,1", encoding='utf8')
        ds = Dataset.from_metadata(str(md))
        assert ds.validate()

        md.write_text(json.replace('COMPS', ', '.join([comp, comp])), encoding='utf8')
        with pytest.raises(ValueError) as excinfo:
            Dataset.from_metadata(str(md))
        assert 'duplicate component' in excinfo.exconly()


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
            'Target_Form_ID': '1'}])
    ds.validate()


def test_add_table(tmpdir, ds):
    ds.add_table('stuff.csv', term_uri('id'), 'col1')
    ds.write(fname=str(tmpdir / 't.json'), **{'stuff.csv': [{'ID': '.ab'}]})
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

    with pytest.raises(NotImplementedError):
        ds.add_foreign_key('foreign.csv', ['fk_id', 'fk_id'], 'primary.csv')

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


def test_log_missing_primary_key(tmpdir, mocker, caplog):
    md = Path(str(tmpdir)) / 'metadata.json'

    ds = Generic.in_dir(md.parent)
    ds.add_table('t1.csv', 'ID', 'Name', primaryKey=['ID', 'Name'])
    t2 = ds.add_table('t2.csv', 'ID', {'name': 'T1_ID', 'separator': ' '})
    t2.add_foreign_key('T1_ID', 't1.csv', 'ID')
    ds.write(md, **{
        't1.csv': [dict(ID='1', Name='Name')],
        't2.csv': [dict(ID='1', T1_ID=['1'])],
    })
    ds.validate(log=logging.getLogger(__name__))
    assert len(caplog.records) == 2


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
    assert ' '.join(ds_wl.get_segments(list(ds_wl['FormTable'])[0])) == 'a bc d e f'


def test_partial_cognates(ds_wl):
    ds_wl['FormTable'].get_column('Segments').separator = '+'
    ds_wl.add_component('CognateTable')
    ds_wl.write(
        FormTable=[
            {'ID': '1',
             'Value': 'form',
             'Form': 'abcdefg',
             'Segments': ['a bc', 'd e f', 'g'],
             'Language_ID': 'l',
             'Parameter_ID': 'p'}
        ],
        CognateTable=[
            {
                'ID': '1',
                'Form_ID': '1',
                'Cognateset_ID': '1',
                'Segment_Slice': ['2:4'],
            }
        ],
    )
    assert ' '.join(ds_wl.get_segments(list(ds_wl['FormTable'])[0])) == 'a bc d e f g'
    assert ' '.join(ds_wl.get_subsequence(list(ds_wl['CognateTable'])[0])) == 'd e f g'


def _make_tg(tmpdir, *tables):
    tg = TableGroup.fromvalue({'tables': list(tables)})
    tg._fname = Path(str(tmpdir / 'md.json'))  # FIXME: clldutils dependency
    return tg


def test_add_component_from_table(ds):
    ds.add_component(Table.fromvalue({
        "url": 'u.csv',
        "dc:conformsTo": "funny#stuff",
        "tableSchema": {"columns": []}}))


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
    Path(str(tmpdir / 'values.csv')).write_text(
        "IDX,Language_ID,Parameter_ID,Value\n1,1,1,1", encoding='utf-8')
    with pytest.raises(ValueError, match='missing columns'):
        ds = Dataset.from_data(str(tmpdir / 'values.csv'))

    # A known file name will determine the CLDF module of the dataset:
    copy(str(data / 'ds1.csv'), str(tmpdir / 'values.csv'))
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
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
        del ds.tablegroup.common_props['dc:conformsTo']
        Dataset.from_metadata(ds.write_metadata())
        assert len(ds.stats()) == 1

    ds.add_table('extra.csv', 'ID')
    ds.write(**{'ValueTable': [], 'extra.csv': []})
    counts = {r[0]: r[2] for r in ds.stats()}
    assert counts['extra.csv'] == 0


def test_Dataset_auto_foreign_keys(tmpdir):
    ds = StructureDataset.in_dir(str(tmpdir), empty_tables=True)
    ds.add_component(
        {
            'url': 'languages.csv',
            'dc:conformsTo': 'http://cldf.clld.org/v1.0/terms.rdf#LanguageTable',
            'tableSchema': {'primaryKey': 'lid'}},
        {'name': 'lid', 'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#id'})
    ds.add_component(
        {
            'url': 'values.csv',
            'dc:conformsTo': 'http://cldf.clld.org/v1.0/terms.rdf#ValueTable',
            'tableSchema': {'primaryKey': 'vid'}},
        {'name': 'vid', 'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#id'},
        {
            'name': 'feature',
            'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#parameterReference'},
        {
            'name': 'language_lid',
            'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#languageReference'},
        {'name': 'value', 'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#value'})
    ds.write(
        LanguageTable=[{'lid': 'spam'}],
        ValueTable=[
            {'vid': '1', 'feature': 'bing', 'language_lid': 'spam', 'value': 'eggs'}])
    ds.validate()


def test_Dataset_from_data_empty_file(tmpdir):
    Path(str(tmpdir / 'values.csv')).write_text('', encoding='utf-8')
    with pytest.raises(ValueError, match='empty data file'):
        Dataset.from_data(str(tmpdir / 'values.csv'))


@pytest.mark.parametrize('cls, expected', [
    (Dataset, Wordlist),
    (Wordlist, Wordlist),
    (ParallelText, ParallelText),
])
def test_Dataset_from_data(tmpdir, cls, expected):
    forms = tmpdir / 'forms.csv'
    forms.write_text('ID,Language_ID,Parameter_ID,Form', encoding='utf-8')
    assert type(cls.from_data(str(forms))) is expected


def test_Dataset_remove_columns(tmpdir):
    ds = StructureDataset.in_dir(str(tmpdir / 'new'))
    ds.add_component('LanguageTable')
    ds.add_foreign_key('ValueTable', 'Value', 'LanguageTable', 'Name')
    ds.remove_columns('languages.csv', 'ID')

    ds.write(
        ValueTable=[{'ID': '1', 'Language_ID': '1', 'Parameter_ID': '1', 'Value': '1'}],
        LanguageTable=[{'Name': 'x'}]
    )
    with pytest.raises(ValueError):
        ds.validate()

    ds.write(
        ValueTable=[{'ID': '1', 'Language_ID': '1', 'Parameter_ID': '1', 'Value': '1'}],
        LanguageTable=[{'Name': '1'}]
    )
    assert ds.validate()


def test_Dataset_remove_table(tmpdir):
    ds = StructureDataset.in_dir(str(tmpdir / 'new'))
    ds.add_component('LanguageTable')
    ds.add_component('ParameterTable')
    ds.write(
        ValueTable=[{'ID': '1', 'Language_ID': '1', 'Parameter_ID': 1, 'Value': 1}],
        LanguageTable=[{'ID': '1', 'Name': 'l'}],
        ParameterTable=[{'ID': '1', 'Name': 'l'}],
    )
    assert ds.validate()

    ds.remove_table('LanguageTable')

    # Make sure other foreign key constraints are still enforced:
    ds.write(
        ValueTable=[{'ID': '1', 'Language_ID': '1', 'Parameter_ID': 1, 'Value': 1}],
        ParameterTable=[{'ID': 'x', 'Name': 'l'}],
    )
    with pytest.raises(ValueError):
        ds.validate()

    # But foreign keys into the removed table are not:
    ds.write(
        ValueTable=[{'ID': '1', 'Language_ID': '1', 'Parameter_ID': 1, 'Value': 1}],
        ParameterTable=[{'ID': '1', 'Name': 'l'}],
    )
    assert ds.validate()


def test_Dataset_validate(tmpdir, mocker, caplog):
    ds = StructureDataset.in_dir(str(tmpdir / 'new'))
    ds.write(ValueTable=[])
    values = tmpdir / 'new' / 'values.csv'
    assert values.check()
    Path(str(values)).unlink()
    assert not ds.validate(log=logging.getLogger(__name__))
    assert caplog.records

    ds.write(ValueTable=[])
    assert ds.validate()

    ds['ValueTable'].tableSchema.columns = []
    with pytest.raises(ValueError):
        ds.validate()
    assert not ds.validate(log=mocker.Mock())
    ds.tablegroup.tables = []
    with pytest.raises(ValueError):
        ds.validate()

    ds = StructureDataset.in_dir(str(tmpdir / 'new'))
    ds.add_component('LanguageTable')
    ds.write(ValueTable=[], LanguageTable=[])
    assert ds.validate()

    # test violation of referential integrity:
    ds.write(ValueTable=[{'ID': '1', 'Value': '1', 'Language_ID': 'lid', 'Parameter_ID': 'pid'}], LanguageTable=[])
    assert not ds.validate(log=mocker.Mock())

    # test an invalid CLDF URL:
    ds['LanguageTable'].common_props['dc:conformsTo'] = 'http://cldf.clld.org/404'
    with pytest.raises(ValueError):
        ds.validate()

    ds = StructureDataset.in_dir(str(tmpdir / 'new'))
    ds['ValueTable'].get_column('Source').propertyUrl = URITemplate(
        'http://cldf.clld.org/404')
    ds.write(ValueTable=[])
    with pytest.raises(ValueError):
        ds.validate()


def test_Dataset_validate_custom_validator(tmpdir):
    ds = StructureDataset.in_dir(str(tmpdir / 'new'))
    ds.write(ValueTable=[
        {'ID': '1', 'Value': 'x', 'Language_ID': 'l', 'Parameter_ID': 'p'}])
    assert ds.validate()

    def v(tg, t, c, r):
        if r[c.name] == 'x':
            raise ValueError()

    with pytest.raises(ValueError):
        ds.validate(validators=[('ValueTable', 'Value', v)])


def test_Dataset_validate_missing_table(tmpdir, caplog):
    ds = StructureDataset.from_metadata(str(tmpdir))
    ds.tablegroup.tables = []
    ds.write()
    ds.validate(log=logging.getLogger(__name__))
    assert caplog.records


@pytest.mark.filterwarnings('ignore::UserWarning')
def test_Dataset_validate_duplicate_columns(data, caplog):
    ds = Dataset.from_metadata(data / 'dataset_with_duplicate_columns' / 'metadata.json')
    ds.validate(log=logging.getLogger(__name__))
    assert len(caplog.records) == 2 and all('uplicate' in r.message for r in caplog.records)


def test_stats(dataset):
    assert dict([(r[0], r[2]) for r in dataset.stats()])['ds1.csv'] == 5
    assert dict([(r[0], r[2]) for r in dataset.stats(exact=True)])['ds1.csv'] == 2


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
    ds2 = StructureDataset.from_metadata(
        str(tmpdir.join('StructureDataset-metadata.json')))
    assert ds2['ValueTable'].common_props['dc:extent'] == 1
    assert {s[1]: s[2] for s in ds.stats()}['ValueTable'] == 1
    ds['ValueTable'].common_props['dc:extent'] = 3
    assert {s[1]: s[2] for s in ds.stats()}['ValueTable'] == 3
    with pytest.raises(ValueError):
        ds.validate()
    ds.sources.add("@misc{key,\ntitle={the title}\n}")
    ds.write(ValueTable=(
        {
            'ID': '1',
            'Language_ID': 'abcd1234',
            'Parameter_ID': 'f1',
            'Value': 'yes',
            'Source': ['key[1-20]'],
        } for _ in range(1)))
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
            'Primary_Text': 'si',
            'Translated_Text': 'yes',
            'Analyzed_Word': ['morph1', 'morph2', 'morph3'],
            'Gloss': ['gl1', 'gl2', 'gl3'],
        }])
    ds.validate()


def test_validators(tmpdir, data, caplog):
    copy(str(data / 'invalid.csv'), str(tmpdir / 'values.csv'))
    ds = Dataset.from_data(str(tmpdir / 'values.csv'))

    with pytest.raises(ValueError):
        ds.validate()

    log = logging.getLogger(__name__)
    ds.validate(log=log)
    assert len(caplog.records) == 2

    for col in ds.tablegroup.tables[0].tableSchema.columns:
        if col.name == 'Language_ID':
            col.propertyUrl.uri = 'http://cldf.clld.org/v1.0/terms.rdf#glottocode'

    ds.validate(log=log)
    assert len(caplog.records) == 6


def test_get_modules():
    assert not get_modules()[0].match(5)


@pytest.mark.filterwarnings('ignore::UserWarning')
def test_iter_datasets(data, tmpdir):
    assert len(list(iter_datasets(data))) == 8

    tmpdir.join('f1').write_text('äöü', encoding='latin1')
    tmpdir.join('f2').write_text('{x', encoding='utf8')
    tmpdir.join('f3').write_text('{}', encoding='utf8')
    assert len(list(iter_datasets(Path(str(tmpdir))))) == 0


def test_Dataset_iter_rows(dataset):
    for row in dataset.iter_rows('ValueTable', 'parameterReference', 'languageReference'):
        assert all(k in row for k in ['parameterReference', 'languageReference'])
        assert row['languageReference'] == 'abcd1234'


def test_Dataset_get_row_url(data):
    dataset = Dataset.from_metadata(data / 'ds1.csv-metadata.json')
    assert dataset.get_row_url('ValueTable', '2') is None

    dataset = Dataset.from_metadata(data / 'ds1.csv-metadata.json')
    dataset['ValueTable', 'id'].valueUrl = URITemplate('http://example.org/{ID}')
    assert dataset.get_row_url('ValueTable', '1') == 'http://example.org/1'

    dataset = Dataset.from_metadata(data / 'ds1.csv-metadata.json')
    dataset['ValueTable', 'languageReference'].datatype = Datatype.fromvalue('anyURI')
    assert dataset.get_row_url('ValueTable', '1') == 'abcd1234'


def test_Dataset_from_url(urlopen):
    ds = Dataset.from_metadata('http://example.org/ds1.csv-metadata.json')
    assert ds.bibpath == 'http://example.org/ds1.bib'
    assert ds.bibname == 'ds1.bib'
    assert len(ds.sources) == 3
