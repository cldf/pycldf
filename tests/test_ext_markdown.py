import pytest

from pycldf.ext.markdown import *


def test_CLDFMarkdownLink(structuredataset_with_examples, dataset):
    link = CLDFMarkdownLink.from_component('ExampleTable', objid='e1')
    assert link.url_without_fragment == 'ExampleTable'
    assert link.component() == 'ExampleTable'
    assert link.get_object(structuredataset_with_examples).id == 'e1'
    assert link.get_row(structuredataset_with_examples)['ID'] == 'e1'

    link = CLDFMarkdownLink('label', 'ds1.bib#cldf:meier2015')
    assert link.component(dataset) == 'Source'

    link = CLDFMarkdownLink('label', 'xy.csv#cldf:meier2015')
    assert link.component(dataset) is None


def test_CLDFMarkdownText(structuredataset_with_examples, tmp_path):
    class Renderer(CLDFMarkdownText):
        def render_link(self, cldf_link):
            return '++' + cldf_link.get_object(self.dataset_mapping).id + '++'

    assert '++e1++' == Renderer(
        '[](ExampleTable#cldf-d:e1)',
        dataset_mapping={'d': structuredataset_with_examples}).render()
    tmp_path.joinpath('test.md').write_text("""
---
cldf-datasets: {}#dc:conformsTo=http://cldf.clld.org/v1.0/terms.rdf#StructureDataset
---
[](ExampleTable#cldf:e1)
""".format(structuredataset_with_examples.directory / 'metadata.json'), encoding='utf8')
    assert '++e1++' == Renderer(tmp_path / 'test.md').render()

    #
    # FIXME: test with two datasets!
    #

    renderer = FilenameToComponent(
        '[y](other/path) [x](a/examples.csv#cldf:e1)',
        dataset_mapping=structuredataset_with_examples)
    assert renderer.render() == '[y](other/path) [x](ExampleTable#cldf:e1)'

    with pytest.raises(ValueError):
        _ = Renderer('text', dataset_mapping={'-': structuredataset_with_examples})
