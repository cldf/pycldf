import pytest

from pycldf import Dataset


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


def test_Object(dataset2):
    c = dataset2.get_object('ParameterTable', 'c1')
    assert c.aboutUrl() == 'http://example.org/Hand'
    assert c.valueUrl() == 'http://example.org/c1/Hand'
    assert isinstance(c.propertyUrl(), str)


def test_Object_references(dataset):
    o = dataset.get_object('ValueTable', '1')
    assert len(o.references) == 2

