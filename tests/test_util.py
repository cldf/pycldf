from __future__ import unicode_literals

import pytest


@pytest.mark.parametrize("sliceable,slices,expected", [
    ('abcdefg', ['2:5', (1, 4)], 'bcdebcd'),
    ([1, 2, 3, 4], ['1:6:2'], [1, 3]),
    ((1, 2, 3, 4), ['1:6:2'], (1, 3))
])
def test_multislice(sliceable, slices, expected):
    from pycldf.util import multislice

    assert multislice(sliceable, *slices) == expected
