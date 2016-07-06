# coding: utf8
from __future__ import unicode_literals, print_function, division

from clldutils.path import Path

from pycldf.dataset import Dataset, REQUIRED_FIELDS

FIXTURES = Path(__file__).parent.joinpath('fixtures')


def make_dataset(name='test', fields=None, rows=None):
    ds = Dataset(name)
    ds.fields = fields or tuple(f[0] for f in REQUIRED_FIELDS)
    if rows:
        for row in rows:
            ds.add_row(row)
    return ds
