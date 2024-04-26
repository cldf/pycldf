import json
import types
import decimal

import pytest

import rfc3986
from csvw.metadata import URITemplate
from pycldf import Dataset, Generic, StructureDataset
from pycldf.orm import Language


@pytest.mark.parametrize(
    'input,output',
    [
        (None, None),
        ([], []),
        ({}, {}),
        (decimal.Decimal(1), 1),
        ({'k': [None]}, {'k': [None]}),
        (types.SimpleNamespace(a=3), 'namespace(a=3)')
    ]
)
def test_to_json(input, output):
    from pycldf.orm import to_json
    assert to_json(input) == output


@pytest.fixture
def dataset2(data):
    dsdir = data / 'dataset_with_listvalued_foreign_keys_to_component'
    return Dataset.from_metadata(dsdir / 'metadata.json')


def test_listvaluedrelations(dataset2):
    form = dataset2.objects('FormTable')[0]
    assert form.language and len(form.languages) == 1
    assert len(form.parameters) == 2
    assert dataset2.get_object('ParameterTable', form.parameters[0].id) in form.parameters
    assert dataset2.objects('ParameterTable')[0] == dataset2.objects('ParameterTable')['c1']
    _ = {form.parameters[0]: 4}

    with pytest.raises(ValueError):
        _ = form.parameter


def test_custom_object_class(dataset2):
    class Variety(Language):
        __component__ = 'LanguageTable'
        def id_name(self):
            return '{} [{}]'.format(self.name, self.id)

    obj = dataset2.get_object('LanguageTable', 'l1', Variety)
    assert obj.id_name() == 'Language 1 [l1]'
    assert 'Variety' in repr(obj)


def test_Object(dataset2):
    c = dataset2.get_object('ParameterTable', 'c1')
    assert c.aboutUrl() == 'http://example.org/Hand'
    assert c.valueUrl() == 'http://example.org/c1/Hand'
    assert isinstance(c.propertyUrl(), str)


def test_Object_references(dataset):
    o = dataset.get_object('ValueTable', '1')
    assert len(o.references) == 2


def test_cognacy(wordlist_with_cognates):
    cog = wordlist_with_cognates.get_object('CognateTable', 'c1')
    assert cog.form and cog.cognateset
    assert cog in cog.cognateset.cognates
    assert not wordlist_with_cognates.get_object('ParameterTable', 'p2').all_related(
        'concepticonReference')


def test_borrowings(wordlist_with_borrowings):
    b = wordlist_with_borrowings.get_object('BorrowingTable', 'b1')
    assert b.targetForm.language != b.sourceForm.language
    assert len(b.targetForm.language.forms) == 1
    assert len(b.targetForm.parameter.forms) == 2


def test_parameter(structuredataset_with_examples):
    p = structuredataset_with_examples.get_object('ParameterTable', 'p1')
    assert len(p.codes) == 2


def test_examples(structuredataset_with_examples):
    v = structuredataset_with_examples.get_object('ValueTable', 'v1')
    assert v.parameter and v.language
    assert len(v.examples) == 2
    ex = v.examples['e1']
    assert 'der inhalt' in ex.igt
    assert ex.language != ex.metaLanguage
    assert v.code.name == 'Yes' and v.cldf.value == 'ja'
    assert isinstance(v.language.as_geojson_feature, dict)
    assert v.language.as_geojson_feature['properties']['name']
    assert json.dumps(v.language.as_geojson_feature)
    assert len(v.language.values) == 2
    assert len(v.parameter.values) == 1


def test_dictionary(dictionary):
    senses = dictionary.objects('SenseTable')
    assert senses[0].entry
    assert len(dictionary.get_object('EntryTable', '2').senses) == 2


def test_catalogs(wordlist_with_cognates, glottolog_repos, concepticon_repos):
    from pyglottolog import Glottolog
    from pyconcepticon import Concepticon

    l = wordlist_with_cognates.get_object('LanguageTable', 'l1')
    gl = Glottolog(glottolog_repos)
    assert l.glottolog_languoid(gl).id == 'abcd1234'
    assert l.glottolog_languoid({g.id: g for g in gl.languoids()}).id == 'abcd1234'

    c = wordlist_with_cognates.get_object('ParameterTable', 'p1')
    conc = Concepticon(concepticon_repos)
    assert c.concepticon_conceptset(conc).gloss == 'CONTEMPTIBLE'
    assert c.concepticon_conceptset(conc.conceptsets).gloss == 'CONTEMPTIBLE'


def test_Media(tmp_path):
    ds = Generic.in_dir(tmp_path)
    ds.add_component('MediaTable')
    ds.remove_columns('MediaTable', 'downloadUrl')
    ds['MediaTable', 'id'].valueUrl = URITemplate('http://example.org/{ID}')
    ds.write(MediaTable=[dict(ID='1', Media_Type='text/plain')])
    assert ds.get_object('MediaTable', '1').downloadUrl == 'http://example.org/1'

    ds = Generic.in_dir(tmp_path)
    ds.add_component('MediaTable')
    ds['MediaTable', 'id'].valueUrl = URITemplate('http://example.org/{ID}')
    url = rfc3986.URIReference.from_string('http://example.org/ü')
    ds.write(
        MediaTable=[dict(ID='1', Media_Type='text/plain', Download_URL=url)])
    assert ds.get_object('MediaTable', '1').downloadUrl == 'http://example.org/ü'


def test_non_id_fk(tmp_path):
    """
    Foreign keys do not have to reference the id column, but may reference another column if it is
    specified as primary key.
    """
    ds = StructureDataset.in_dir(tmp_path)
    ds.add_component('ParameterTable')
    ds['ParameterTable'].tableSchema.primaryKey = ['Name']
    ds['ValueTable'].tableSchema.foreignKeys[0].reference.columnReference = ['Name']
    ds.write(
        ParameterTable=[dict(ID='1', Name='a'), dict(ID='2', Name='b')],
        ValueTable=[
            dict(ID='1', Language_ID='l', Parameter_ID='a', Value='1'),
            dict(ID='2', Language_ID='l', Parameter_ID='b', Value='3'),
        ],
    )
    assert ds.validate()
    assert ds.get_object('ValueTable', '1').parameter.id == '1'

    ds = StructureDataset.in_dir(tmp_path)
    ds.add_component('ParameterTable')
    ds['ValueTable'].tableSchema.foreignKeys[0].reference.columnReference = ['Name']
    ds.write(
        ParameterTable=[dict(ID='1', Name='a'), dict(ID='2', Name='b')],
        ValueTable=[
            dict(ID='1', Language_ID='l', Parameter_ID='a', Value='1'),
            dict(ID='2', Language_ID='l', Parameter_ID='b', Value='3'),
        ],
    )
    assert ds.validate()
    with pytest.raises(NotImplementedError):
        _ = ds.get_object('ValueTable', '1').parameter


def test_typed_parameters(tmp_path):
    from csvw.metadata import Datatype
    dt = Datatype.fromvalue(dict(base='integer', maximum=5))
    ds = StructureDataset.in_dir(tmp_path)
    ds.add_component(
        'ParameterTable',
        {"name": "datatype", "datatype": "json"},
    )
    ds.write(
        ParameterTable=[
            dict(ID='1', datatype=dt.asdict()),
            dict(ID='2'),
            dict(ID='3', datatype='json'),
            dict(ID='4', datatype=dict(base='string', format='a|b|c'))
        ],
        ValueTable=[
            dict(ID='1', Language_ID='l', Parameter_ID='1', Value=dt.formatted(3)),
            dict(ID='2', Language_ID='l', Parameter_ID='2', Value='3'),
            dict(ID='3', Language_ID='l', Parameter_ID='3', Value=json.dumps({'a': 5})),
            dict(ID='4', Language_ID='l', Parameter_ID='4', Value='x'),
        ],
    )
    for v in ds.objects('ValueTable'):
        if v.id == '1':
            assert v.typed_value == 3
        elif v.id == '2':
            assert v.typed_value == '3'
        elif v.id == '3':
            assert v.typed_value['a'] == 5
        elif v.id == '4':
            with pytest.raises(ValueError):
                _ = v.typed_value


def test_columnspec(tmp_path):
    from csvw.metadata import Column
    cs = Column.fromvalue(dict(datatype=dict(base='integer', maximum=5), separator=' '))
    ds = StructureDataset.in_dir(tmp_path)
    ds.add_component('ParameterTable')
    ds.write(
        ParameterTable=[dict(ID='1', ColumnSpec=cs.asdict())],
        ValueTable=[dict(ID='1', Language_ID='l', Parameter_ID='1', Value=cs.write([1, 2, 3]))],
    )
    v = ds.objects('ValueTable')[0]
    assert v.cldf.value == '1 2 3'
    assert v.typed_value == [1, 2, 3]


def test_TextCorpus(textcorpus):
    assert len(textcorpus.texts) == 2

    e = textcorpus.get_object('ExampleTable', 'e2')
    assert e.alternative_translations

    text = e.text
    assert text
    assert text.sentences[0].id == 'e2'

    assert textcorpus.get_text('2').sentences == []

    assert len(textcorpus.sentences) == 2
    assert textcorpus.sentences[0].cldf.primaryText == 'first line'

    with pytest.raises(ValueError) as e:
        textcorpus.validate()
    assert 'ungrammatical' in str(e)


def test_speakerArea(dataset_with_media):
    lang = dataset_with_media.objects('LanguageTable')[0]
    sa = lang.speaker_area
    assert sa.scheme == 'file'
    assert sa
    assert sa.mimetype.subtype == 'geo+json'
    assert 'properties' in lang.speaker_area_as_geojson_feature
    assert json.dumps(lang.speaker_area_as_geojson_feature)
    assert dataset_with_media.objects('LanguageTable')[1].speaker_area_as_geojson_feature
