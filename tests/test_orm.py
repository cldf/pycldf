import pytest

from pycldf import Dataset
from pycldf.orm import Language


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

    assert dataset2.get_object('LanguageTable', 'l1', Variety).id_name() == 'Language 1 [l1]'


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
    assert not wordlist_with_cognates.get_object('ParameterTable', 'p2').all_related(
        'concepticonReference')


def test_borrowings(wordlist_with_borrowings):
    b = wordlist_with_borrowings.get_object('BorrowingTable', 'b1')
    assert b.targetForm.language != b.sourceForm.language
    assert len(b.targetForm.language.forms) == 1
    assert len(b.targetForm.parameter.forms) == 2


def test_examples(structuredataset_with_examples):
    v = structuredataset_with_examples.get_object('ValueTable', 'v1')
    assert v.parameter and v.language
    assert len(v.examples) == 2
    ex = v.examples['e1']
    assert 'der inhalt' in ex.igt
    assert ex.language != ex.metaLanguage
    assert v.code.name == 'Yes' and v.cldf.value == 'ja'
    assert isinstance(v.language.as_geojson_feature, dict)
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
