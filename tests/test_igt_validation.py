import logging

import pytest

from pycldf import TextCorpus


@pytest.mark.parametrize('column', ['Analyzed_Word', 'Gloss'])
@pytest.mark.parametrize('omit_other_column', [False, True])
@pytest.mark.parametrize('rename', [False, True])
def test_missing_igt_token(tmp_path, caplog, column, omit_other_column, rename):
    ds = TextCorpus.in_dir(tmp_path)
    other = 'Gloss' if column == 'Analyzed_Word' else 'Analyzed_Word'
    if omit_other_column:
        ds.remove_columns('ExampleTable', other)
    if rename:
        ds.rename_column('ExampleTable', column, 'Custom_Tokens')
        column = 'Custom_Tokens'
    row = dict(ID='1', Language_ID='lng', Primary_Text='x Person z')
    if not omit_other_column:
        row[other] = ['x', '…', 'z']
    row[column] = ['x', None, 'z']
    ds.write(ExampleTable=[row])

    with pytest.raises(ValueError, match='empty word/gloss token'):
        ds.validate()
    with caplog.at_level(logging.WARNING):
        assert not ds.validate(log=logging.getLogger('igt-test'))
    assert 'empty word/gloss token' in caplog.text


@pytest.mark.parametrize('words,glosses', [
    ([], []),
    (['x', '…', 'z'], ['X', '…', 'Z']),
    (['x', 'y'], []),
])
def test_optional_igt_fields_and_ellipsis(tmp_path, words, glosses):
    ds = TextCorpus.in_dir(tmp_path)
    ds.write(ExampleTable=[dict(
        ID='1', Language_ID='lng', Primary_Text='x Person z',
        Analyzed_Word=words, Gloss=glosses)])
    assert ds.validate()
