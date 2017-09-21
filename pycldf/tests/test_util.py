# coding: utf8
from __future__ import unicode_literals, print_function, division

from nose.tools import assert_equal

from pycldf.util import multislice


def check_multislice(sliceable, slices, expected):
    assert_equal(multislice(sliceable, *slices), expected)


def test_multislice():
    for sliceable, slices, expected in [
        ('abcdefg', ['1:4', (1, 4)], 'bcdbcd'),
        ([1, 2, 3, 4], ['0:5:2'], [1, 3]),
        ((1, 2, 3, 4), ['0:5:2'], (1, 3))
    ]:
        yield check_multislice, sliceable, slices, expected
