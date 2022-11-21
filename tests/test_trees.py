import logging

from pycldf import Generic
from pycldf.trees import *


def test_Trees(dataset_with_trees):
    trees = TreeTable(dataset_with_trees)
    t = list(trees)
    assert len(t) == 2
    assert t[0].newick() and t[1].newick()
    assert trees.validate()


def test_Trees_validate(tmp_path, caplog):
    ds = Generic.in_dir(tmp_path)
    ds.add_component('LanguageTable')
    ds.add_component('TreeTable')
    ds.add_component('MediaTable')
    ds.write(
        LanguageTable=[dict(ID='l1')],
        TreeTable=[
            dict(ID='t', Media_ID='m'),
            dict(ID='t', Name='1', Media_ID='m'),
            dict(ID='t', Name='x', Media_ID='n', Tree_Is_Rooted=True),
        ],
        MediaTable=[
            dict(ID='m', Media_Type='text/x-nh', Download_URL='file:///test.nwk'),
            dict(ID='n', Media_Type='text/plain', Download_URL='file:///test.nex'),
        ],
    )
    tmp_path.joinpath('test.nwk').write_text('(l1,l2);', encoding='utf8')
    tmp_path.joinpath('test.nex').write_text(
        '#NEXUS\n\nbegin trees;\ntree x = [&U](l1,l2);\nend;', encoding='utf8')
    TreeTable(ds).validate(log=logging.getLogger('test'))
    assert len(caplog.records) == 3
    assert caplog.records[0].message.startswith('No newick')
    assert caplog.records[1].message.startswith('Newick node label')
    assert caplog.records[2].message.startswith('conflicting')
    assert not ds.validate(log=logging.getLogger('test'))
