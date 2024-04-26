import shutil
import logging
import zipfile
import warnings
import mimetypes
import contextlib

import pytest

from csvw.metadata import TableGroup, ForeignKey, URITemplate, Column, Table, Link, Datatype

from pycldf.terms import term_uri, TERMS
from pycldf.dataset import (
    Generic, Wordlist, StructureDataset, Dictionary, ParallelText, Dataset, TextCorpus,
    GitRepository, make_column, get_modules, iter_datasets, SchemaError)
from pycldf.sources import Sources


@pytest.fixture
def ds(tmp_path):
    return Generic.in_dir(tmp_path)


@pytest.fixture
def ds_wl(tmp_path):
    return Wordlist.in_dir(tmp_path)


@pytest.fixture
def ds_tc(tmp_path):
    return TextCorpus.in_dir(tmp_path)


@pytest.fixture
def ds_wl_notables(tmp_path):
    return Wordlist.in_dir(str(tmp_path), empty_tables=True)


@pytest.mark.parametrize("col_spec,datatype", [
    ('name', 'string'),
    ({'name': 'num', 'datatype': 'decimal'}, 'decimal'),
    (term_uri('latitude'), 'decimal'),
])
def test_column_basetype(col_spec, datatype):
    assert make_column(col_spec).datatype.base == datatype


def test_v1_1(ds):
    ds.add_component('MediaTable')
    ds.add_component('ContributionTable')
    assert ds['MediaTable', 'mediaType']
    assert ds['ContributionTable', 'citation']


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


def test_provenance(ds, tmp_path):
    ds.add_provenance(wasDerivedFrom=[GitRepository('http://u:p@example.org'), 'other'])
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
    ds.add_provenance(wasDerivedFrom=GitRepository('http://example.org', clone=tmp_path))
    assert ds.properties['prov:wasDerivedFrom']['dc:created']


def test_primary_table(ds, ds_tc):
    assert ds.primary_table is None
    assert ds_tc.primary_table is not None


def test_components(ds):
    ds.add_component('LanguageTable')
    ds.add_table('custom1.csv', 'id', **{'dc:conformsTo': None})
    ds.add_table('custom2.csv', 'id', **{'dc:conformsTo': 'http://example.org'})
    assert len(ds.components) == 1


def test_column_access(ds):
    with pytest.raises(KeyError):
        assert ds['']

    with pytest.raises(SchemaError):
        _ = ds['ValueTable', 'xyz']

    t = ds.add_component('ValueTable', url='datapoints.csv')
    assert 'ValueTable' in ds
    assert t in ds
    assert not Table.fromvalue({'url': 'abc.csv'}) in ds
    assert ds['ValueTable'] == ds['datapoints.csv']

    assert ('ValueTable', 'colx') not in ds
    with pytest.raises(KeyError):
        assert ds['ValueTable', 'colx']

    with pytest.raises(SchemaError) as e:
        _ = ds['ValueTable', Column.fromvalue({"name": "xyz"})]
    assert "xyz" in str(e) and "datapoints.csv" in str(e)
    t = ds['ValueTable']
    assert all((t, c) in ds for c in t.tableSchema.columns)
    assert ds['ValueTable', 'Language_ID'] == ds['datapoints.csv', 'languageReference']

    del ds['ValueTable', 'Language_ID']
    assert ('ValueTable', 'Language_ID') not in ds

    del ds[t]
    assert 'ValueTable' not in ds


def test_tabletype_none(ds):
    ds.add_table('url', {'name': 'col'})
    ds['url'].common_props['dc:conformsTo'] = None
    assert ds.get_tabletype(ds['url']) is None

    with pytest.raises(ValueError):
        ds.add_table('url')

    # Make sure we can add another table with:
    t = ds.add_component({'url': 'other', 'dc:conformsTo': None})
    assert ds.get_tabletype(t) is None


def test_example_separators(ds):
    ds.add_component('ExampleTable')
    assert ds['ExampleTable', 'Gloss'].separator == '\t'


def test_example_validators(ds):
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
    with pytest.raises(ValueError, match='number of words'):
        ds.validate()


def test_invalid_mimetype(ds, recwarn):
    ds.add_component('MediaTable')
    ds.write(MediaTable=[{
        'ID': '1',
        'Media_Type': mimetypes.guess_type('f.png'),
        'Download_URL': 'http://example.org'}])
    ds.validate()
    assert 'Invalid main part' in str(recwarn.pop(UserWarning).message)


def test_regex_validator_for_listvalued_column(ds):
    ds.add_table(
        'test',
        {
            'name': 'col',
            'separator': ';',
            'datatype': {'base': 'string', 'format': '[a-z]{3}'}
        },
    )
    ds.write(test=[{'col': ['abc', 'abcd']}])
    with pytest.raises(ValueError, match='invalid lexical value for string: abcd'):
        ds.validate()


def test_regex_validator_for_listvalued_column2(ds):
    ds.add_table(
        'test',
        {
            'name': 'col',
            'separator': ';',
            'propertyUrl': 'http://cldf.clld.org/v1.0/terms.rdf#iso639P3code',
            'datatype': {'base': 'string'}
        }
    )
    ds.write(test=[{'col': ['abc', 'abcd']}])
    with pytest.raises(ValueError, match='invalid ISO 639-3 code'):
        ds.validate()


def test_unknown_uritemplate_variable(ds, caplog):
    ds.add_table('test', {'name': 'col', 'valueUrl': 'http://example.org/{xyz}'})
    ds.write(test=[{'col': 'abcd'}])
    ds.validate(log=logging.getLogger(__name__))
    assert 'Unknown variables' in caplog.records[0].msg


def test_duplicate_component(ds, tmp_path):
    # adding a component twice is not possible:
    t = ds.add_component('ValueTable')
    t.url = Link('other.csv')
    with pytest.raises(ValueError):
        ds.add_component('ValueTable')

    # JSON descriptions with duplicate components cannot be read:
    md = tmp_path / 'md.json'
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
        (tmp_path / 'values.csv').write_text(
            "ID,Language_ID,Parameter_ID,Value\n1,1,1,1", encoding='utf8')
        ds = Dataset.from_metadata(str(md))
        assert ds.validate()

        md.write_text(json.replace('COMPS', ', '.join([comp, comp])), encoding='utf8')
        with pytest.raises(ValueError, match='duplicate component'):
            Dataset.from_metadata(str(md))


def test_with_zipped_table(ds, data, tmp_path, caplog):
    from pycldf.db import Database

    ds.add_component('LanguageTable')
    md = ds.write(LanguageTable=[dict(ID='l1')], zipped=['LanguageTable'])
    assert md.parent.joinpath('languages.csv.zip').exists()
    assert len(list(Dataset.from_metadata(md)['LanguageTable'])) == 1

    dsdir = tmp_path / 'ds'
    shutil.copytree(data / 'structuredataset_with_examples', dsdir)
    ds = Dataset.from_metadata(dsdir / 'metadata.json')
    assert ds.validate()

    # Now replace a table with its zipped content:
    with zipfile.ZipFile(dsdir / 'values.csv.zip', 'w') as zipf:
        zipf.write(dsdir / 'values.csv', arcname='values.csv')
    dsdir.joinpath('values.csv').unlink()
    assert not dsdir.joinpath('values.csv').exists()

    assert len(ds.objects('ValueTable')) == 3
    with caplog.at_level(logging.INFO):
        assert ds.validate(log=logging.getLogger(__name__))
    assert 'values.csv' in caplog.records[0].msg
    db = Database(ds, fname=tmp_path / 'db.sqlite')
    db.write_from_tg()
    assert db.query('select count(*) from valuetable')[0][0] == 3


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


def test_add_table(tmp_path, ds):
    ds.add_table('stuff.csv', term_uri('id'), 'col1')
    ds.write(fname=tmp_path / 't.json', **{'stuff.csv': [{'ID': '.ab'}]})
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


def test_log_missing_primary_key(tmp_path, caplog):
    md = tmp_path / 'metadata.json'

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


def _make_tg(tmp_path, *tables):
    tg = TableGroup.fromvalue({'tables': list(tables)})
    tg._fname = tmp_path / 'md.json'
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


def test_modules(tmp_path):
    ds = Dataset(_make_tg(tmp_path))
    assert ds.primary_table is None
    ds = Dataset(_make_tg(tmp_path, {"url": "data.csv"}))
    assert ds.primary_table is None
    ds = Dataset(_make_tg(tmp_path, {
        "url": "data.csv",
        "dc:conformsTo": "http://cldf.clld.org/v1.0/terms.rdf#ValueTable"}))
    assert ds.primary_table == 'ValueTable'
    assert Wordlist.in_dir(tmp_path).primary_table
    assert Dictionary.in_dir(tmp_path).primary_table
    assert StructureDataset.in_dir(tmp_path).primary_table


def test_Dataset_from_scratch(tmp_path, data):
    # An unknown file name cannot be used with Dataset.from_data:
    shutil.copy(data / 'ds1.csv', tmp_path / 'xyz.csv')
    with pytest.raises(ValueError):
        Dataset.from_data(tmp_path / 'xyz.csv')

    # Known file name, but non-standard column name:
    tmp_path.joinpath('values.csv').write_text(
        "IDX,Language_ID,Parameter_ID,Value\n1,1,1,1", encoding='utf-8')
    with pytest.raises(ValueError, match='missing columns'):
        ds = Dataset.from_data(tmp_path / 'values.csv')

    # A known file name will determine the CLDF module of the dataset:
    shutil.copy(data / 'ds1.csv', tmp_path / 'values.csv')
    with warnings.catch_warnings(record=True):
        warnings.simplefilter("always")
        ds = Dataset.from_data(tmp_path / 'values.csv')
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


def test_Dataset_auto_foreign_keys(tmp_path):
    ds = StructureDataset.in_dir(tmp_path, empty_tables=True)
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


def test_Dataset_from_data_empty_file(tmp_path):
    tmp_path.joinpath('values.csv').write_text('', encoding='utf-8')
    with pytest.raises(ValueError, match='empty data file'):
        Dataset.from_data(tmp_path / 'values.csv')


@pytest.mark.parametrize('cls, expected', [
    (Dataset, Wordlist),
    (Wordlist, Wordlist),
    (ParallelText, ParallelText),
])
def test_Dataset_from_data(tmp_path, cls, expected):
    forms = tmp_path / 'forms.csv'
    forms.write_text('ID,Language_ID,Parameter_ID,Form', encoding='utf-8')
    assert type(cls.from_data(forms)) is expected


def test_Dataset_remove_columns(tmp_path):
    ds = StructureDataset.in_dir(tmp_path / 'new')
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


def test_Dataset_remove_table(tmp_path):
    ds = StructureDataset.in_dir(tmp_path / 'new')
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


def test_Dataset_validate(tmp_path, mocker, caplog):
    ds = StructureDataset.in_dir(tmp_path / 'new')
    ds.write(ValueTable=[])
    values = tmp_path / 'new' / 'values.csv'
    assert values.exists()
    values.unlink()
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

    ds = StructureDataset.in_dir(tmp_path / 'new')
    ds.add_component('LanguageTable')
    ds.write(ValueTable=[], LanguageTable=[])
    assert ds.validate()

    # test violation of referential integrity:
    ds.write(ValueTable=[{'ID': '1', 'Value': '1', 'Language_ID': 'lid', 'Parameter_ID': 'pid'}], LanguageTable=[])
    assert not ds.validate(log=mocker.Mock())

    # test an invalid CLDF URL:
    ds['LanguageTable'].common_props['dc:conformsTo'] = 'http://cldf.clld.org/404'
    with pytest.warns(UserWarning):
        with pytest.raises(ValueError):
            ds.validate()

    ds = StructureDataset.in_dir(tmp_path / 'new')
    ds['ValueTable'].get_column('Source').propertyUrl = URITemplate(
        'http://cldf.clld.org/404')
    ds.write(ValueTable=[])
    with pytest.warns(UserWarning):
        with pytest.raises(ValueError):
            ds.validate()


def test_Dataset_cardinality_mismatch(tmp_path):
    ds = StructureDataset.in_dir(tmp_path / 'new')
    ds.write(ValueTable=[{'ID': '1', 'Value': 'x', 'Language_ID': 'l', 'Parameter_ID': 'p'}])
    terms = tmp_path / 'terms.rdf'
    terms.write_text(TERMS._path.read_text(encoding='utf8').replace(
        '<rdfs:subPropertyOf rdf:resource="http://www.w3.org/2000/01/rdf-schema#comment" />',
        '<rdfs:subPropertyOf rdf:resource="http://www.w3.org/2000/01/rdf-schema#comment" />'
        '<dc:extent>multivalued</dc:extent>',
    ))
    assert ds.validate()
    with pytest.raises(ValueError, match='multivalued'):
        ds.validate(ontology_path=terms)

    ds['ValueTable', 'comment'].separator = ';'
    terms.write_text(TERMS._path.read_text(encoding='utf8').replace(
        '<rdfs:subPropertyOf rdf:resource="http://www.w3.org/2000/01/rdf-schema#comment" />',
        '<rdfs:subPropertyOf rdf:resource="http://www.w3.org/2000/01/rdf-schema#comment" />'
        '<dc:extent>singlevalued</dc:extent>',
    ))
    with pytest.raises(ValueError, match='singlevalued'):
        ds.validate(ontology_path=terms)


def test_Dataset_validate_custom_validator(tmp_path):
    ds = StructureDataset.in_dir(tmp_path / 'new')
    ds.write(ValueTable=[
        {'ID': '1', 'Value': 'x', 'Language_ID': 'l', 'Parameter_ID': 'p'}])
    assert ds.validate()

    def v(tg, t, c, r):
        if r[c.name] == 'x':
            raise ValueError()

    with pytest.raises(ValueError):
        ds.validate(validators=[('ValueTable', 'Value', v)])


def test_Dataset_validate_missing_table(tmp_path, caplog):
    ds = StructureDataset.from_metadata(tmp_path)
    ds.tablegroup.tables = []
    ds.write()
    ds.validate(log=logging.getLogger(__name__))
    assert caplog.records


@pytest.mark.filterwarnings('ignore::UserWarning')
def test_Dataset_validate_duplicate_columns(data, caplog, csvw3):
    with contextlib.ExitStack() as stack:
        if csvw3:  # pragma: no cover
            stack.enter_context(pytest.raises(ValueError))
        ds = Dataset.from_metadata(data / 'dataset_with_duplicate_columns' / 'metadata.json')
        ds.validate(log=logging.getLogger(__name__))  # pragma: no cover
    if not csvw3:  # pragma: no cover
        assert len(caplog.records) == 2 and all('uplicate' in r.message for r in caplog.records)


def test_stats(dataset):
    assert dict([(r[0], r[2]) for r in dataset.stats()])['ds1.csv'] == 5
    assert dict([(r[0], r[2]) for r in dataset.stats(exact=True)])['ds1.csv'] == 2


def test_Dataset_write(tmp_path):
    ds = StructureDataset.from_metadata(tmp_path)
    ds.write(ValueTable=[])
    assert (tmp_path / 'values.csv').exists()
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
    ds2 = StructureDataset.from_metadata(tmp_path.joinpath('StructureDataset-metadata.json'))
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


def test_Dataset_zipped_sources(tmp_path):
    ds = StructureDataset.from_metadata(tmp_path)
    ds.add_sources("@misc{ky,\ntitle={the title}\n}")
    ds.write(ValueTable=[
        {
            'ID': '1',
            'Language_ID': 'abcd1234',
            'Parameter_ID': 'f1',
            'Value': 'yes',
            'Source': ['ky'],
        }],
        zipped='sources.bib')
    assert not tmp_path.joinpath('sources.bib').exists()
    assert tmp_path.joinpath('sources.bib.zip').exists()
    ds2 = StructureDataset.from_metadata(tmp_path / 'StructureDataset-metadata.json')
    assert 'ky' in ds2.sources


def test_validators(tmp_path, data, caplog):
    shutil.copy(str(data / 'invalid.csv'), tmp_path / 'values.csv')
    ds = Dataset.from_data(tmp_path / 'values.csv')

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
def test_iter_datasets(data, tmp_path, csvw3, caplog):
    assert len(list(iter_datasets(data))) == 11 if csvw3 else 12

    if csvw3:
        assert 'Reading' in caplog.records[0].msg

    tmp_path.joinpath('f1').write_text('äöü', encoding='latin1')
    tmp_path.joinpath('f2').write_text('{x', encoding='utf8')
    tmp_path.joinpath('f3').write_text('{}', encoding='utf8')
    assert len(list(iter_datasets(tmp_path))) == 0


def test_Dataset_iter_rows(dataset):
    for row in dataset.iter_rows('ValueTable', 'parameterReference', 'languageReference'):
        assert all(k in row for k in ['parameterReference', 'languageReference'])
        assert row['languageReference'] == 'abcd1234'


def test_Dataset_get_row_url(data):
    dataset = Dataset.from_metadata(data / 'ds1.csv-metadata.json')
    assert dataset.filename == 'ds1.csv-metadata.json'
    assert dataset.get_row_url('ValueTable', '2') is None

    dataset = Dataset.from_metadata(data / 'ds1.csv-metadata.json')
    dataset['ValueTable', 'id'].valueUrl = URITemplate('http://example.org/{ID}')
    assert dataset.get_row_url('ValueTable', '1') == 'http://example.org/1'

    dataset = Dataset.from_metadata(data / 'ds1.csv-metadata.json')
    dataset['ValueTable', 'languageReference'].datatype = Datatype.fromvalue('anyURI')
    assert dataset.get_row_url('ValueTable', '1') == 'abcd1234'


def test_Dataset_from_url(urlopen):
    ds = Dataset.from_metadata('http://example.org/ds1.csv-metadata.json')
    assert ds.filename == 'ds1.csv-metadata.json'
    assert ds.bibpath == 'http://example.org/ds1.bib'
    assert ds.bibname == 'ds1.bib'
    assert len(ds.sources) == 3


def test_Dataset_get_foreign_key_target(tmp_path):
    ds = StructureDataset.in_dir(tmp_path)
    ds.add_component('LanguageTable')
    t, c = ds.get_foreign_key_reference('values.csv', 'Language_ID')
    assert ds.get_tabletype(t) == 'LanguageTable'
    assert c.name == 'ID'

    assert ds.get_foreign_key_reference('values.csv', 'Value') is None


def test_Dataset_copy(tmp_path):
    tmp_path.joinpath('data').mkdir()
    ds = StructureDataset.in_dir(tmp_path)
    ds.add_table('data/sets.csv', 'ID', 'Name')
    ds.add_columns('ValueTable', 'Set_ID')
    ds.add_foreign_key('ValueTable', 'Set_ID', 'data/sets.csv', 'ID')
    ds.properties['dc:source'] = 'data/s.bib'
    ds.add_sources('@book{src1,\ntitle="the Book"}')
    ds.write(
        fname=tmp_path / 'meta.js',
        ValueTable=[dict(
            ID='1', Value='x', Language_ID='l', Parameter_ID='p', Source=['src1'], Set_ID='s')],
        **{'data/sets.csv': [dict(ID='s', Name='the set')]}
    )
    ds = Dataset.from_metadata(tmp_path / 'meta.js')
    assert ds.validate()

    dest = tmp_path / 'new' / 'location'
    ds.copy(dest, mdname='md.json')
    copy = Dataset.from_metadata(dest / 'md.json')
    assert copy.validate()

    # Make sure all file references are relative:
    shutil.copytree(dest, tmp_path / 'moved')
    assert Dataset.from_metadata(tmp_path / 'moved' / 'md.json').validate()


def test_Dataset_rename_column(ds):
    lt = ds.add_component('LanguageTable')
    lt.aboutUrl = URITemplate('{#ID}.md')
    vt = ds.add_component('ValueTable')
    ds.rename_column(lt, 'ID', 'X')
    ds.rename_column(lt, 'Glottocode', 'GC')
    assert '{GC}' in str(ds['LanguageTable', 'glottocode'].valueUrl)
    assert '{#X}.md' == str(ds['LanguageTable'].aboutUrl)
    ds.rename_column(vt, 'Language_ID', '')
    assert ds['LanguageTable', 'id'].name == 'X'
    assert lt.tableSchema.primaryKey == ['X']
    lfk = [
        fk.reference for fk in ds['ValueTable'].tableSchema.foreignKeys
        if fk.reference.resource == lt.url][0]
    assert 'X' in lfk.columnReference
    # Adding a component after the renaming will also respect the new name:
    ex = ds.add_component('ExampleTable')
    lfk = [
        fk.reference for fk in ex.tableSchema.foreignKeys
        if 'Language_ID' in fk.columnReference][0]
    assert 'X' in lfk.columnReference


def test_Dataset_set_sources(ds):
    assert isinstance(ds.sources, Sources)
    with pytest.raises(TypeError):
        ds.sources = 5
    src = Sources()
    ds.sources = src
    assert ds.sources is src


def test_StructureDataset(structuredataset_with_examples):
    assert len(structuredataset_with_examples.features) == 2
