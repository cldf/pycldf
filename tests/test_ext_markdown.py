import pytest

from pycldf import Dataset
from pycldf.ext.markdown import *


def test_CLDFMarkdownLink(structuredataset_with_examples):
    link = CLDFMarkdownLink.from_component('ExampleTable', objid='e1')
    assert link.url_without_fragment == 'ExampleTable'
    assert link.get_object(structuredataset_with_examples).id == 'e1'
    assert link.get_row(structuredataset_with_examples)['ID'] == 'e1'


@pytest.mark.parametrize(
    'url,component,with_ds',
    [
        ('TreeTable#cldf:t1', 'TreeTable', None),
        ('TreeTable#cldf:t1', None, 'ex'),
        ('xy.csv#cldf:t1', None, 'ex'),
        ('ExampleTable#cldf:t1', 'ExampleTable', 'ex'),
        ('examples.csv#cldf:t1', 'ExampleTable', 'ex'),
        ('ds1.bib#cldf:meier2015', 'Source', 'ds'),
        ('metadata.json#cldf:__all__', 'Metadata', 'ex'),
    ]
)
def test_CLDFMarkdownLink_component(
        url, component, with_ds, structuredataset_with_examples, dataset):
    link = CLDFMarkdownLink('l', url)
    ds = dataset if with_ds == 'ds' else \
        (structuredataset_with_examples if with_ds == 'ex' else None)
    assert link.component(cldf=ds) == component


def test_CLDFMarkdownText(structuredataset_with_examples, dataset, tmp_path):
    class Renderer(CLDFMarkdownText):
        def render_link(self, cldf_link):
            obj = self.get_object(cldf_link)
            if hasattr(obj, 'id'):
                assert obj.id == cldf_link.get_object(self.dataset_mapping).id
            return '++' + (obj.id if hasattr(obj, 'id') else str(obj)) + '++'

    renderer = Renderer(
        '[](ExampleTable#cldf-d:e1)',
        dataset_mapping={'d': structuredataset_with_examples})
    assert len(renderer.dataset_mapping) == 1
    assert len(renderer.dataset_mapping.items()) == 1
    assert [k for k in renderer.dataset_mapping] == ['d']
    with pytest.raises(TypeError):
        renderer.dataset_mapping['d'] = 5
    assert '++e1++' == renderer.render()
    res = Renderer(
        '[](Metadata#cldf:__all__)', dataset_mapping=structuredataset_with_examples).render()
    assert '@context' in res

    tmp_path.joinpath('test.md').write_text("""
---
cldf-datasets: {}#dc:conformsTo=http://cldf.clld.org/v1.0/terms.rdf#StructureDataset
---
[](ExampleTable#cldf:e1)
[](metadata.json#cldf:"rdf:ID")
""".format(structuredataset_with_examples.directory / 'metadata.json'), encoding='utf8')
    r = Renderer(tmp_path / 'test.md')
    assert '++e1++\n++with_ex++' == r.render()
    assert r.frontmatter.startswith('---') and 'cldf-datasets' in r.frontmatter

    #
    # FIXME: test with two datasets!
    #


    with pytest.raises(ValueError):
        _ = Renderer('text', dataset_mapping={'-': structuredataset_with_examples})


@pytest.mark.parametrize(
    'cldfmd,expected',
    [
        ('[](custom.csv#cldf:1)', 'custom.csv'),
        ('[](metadata.json?with_params#cldf:"dc:title")', 'Metadata?with_params'),
        ('[y](other/path) [x](a/concepts.csv?p#cldf:e1)',
         '[y](other/path) [x](ParameterTable?p#cldf:e1)'),
    ]
)
def test_FilenameToComponent(cldfmd, expected, data):
    ds = Dataset.from_metadata(
        data / 'dataset_with_listvalued_foreign_keys_to_component' / 'metadata.json')
    assert expected in FilenameToComponent(cldfmd, ds).render()
