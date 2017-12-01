# coding: utf8
from __future__ import unicode_literals, print_function, division

import pytest
from clldutils.path import Path, read_text

from pycldf.dataset import ParallelText


@pytest.fixture
def ds(tmpdir):
    ds = ParallelText.in_dir(str(tmpdir))
    ds.add_component('FunctionalEquivalentTable')
    ds.add_component('FunctionalEquivalentsetTable')
    for fname in [
        'forms.csv',
        'functionalEquivalents.csv',
        'functionalEquivalentsets.csv',
    ]:
        src = Path(__file__).parent / 'data' / 'paralleltext_{0}'.format(fname)
        target = tmpdir.join(fname)
        target.write(read_text(src).encode('utf8'), mode='wb')
    return ds


def test_paralleltext(ds):
    ds.validate()
    assert len(list(ds[ds.primary_table])) == 9


def test_get_equivalent(ds):
    for fes in ds['FunctionalEquivalentsetTable']:
        if fes['Description'] == 'Jesus Christ':
            break
    else:
        raise ValueError

    equiv = [
        ds.get_equivalent(r) for r in ds['FunctionalEquivalentTable']
        if r['FunctionalEquivalentset_ID'] == fes['ID']]
    assert equiv == [['Jesu'], ['Jisas\u0268', 'Kiraisoy\xe1'], ['Jisas', 'Krais']]
