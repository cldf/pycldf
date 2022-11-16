import pytest

from pycldf.util import *


@pytest.mark.parametrize("sliceable,slices,expected", [
    ('abcdefg', ['2:5', (1, 4)], 'bcdebcd'),
    ([1, 2, 3, 4], ['1:6:2'], [1, 3]),
    ((1, 2, 3, 4), ['1:6:2'], (1, 3))
])
def test_multislice(sliceable, slices, expected):
    assert multislice(sliceable, *slices) == expected


def test_DictTuple():
    t = DictTuple([1, 2, 3], key=lambda i: str(i + 1))
    assert t['4'] == t[2] == 3

    t = DictTuple([1, 2, 3, 4, 3, 2], key=lambda i: str(i), multi=True)
    assert t['2'] == [2, 2]


@pytest.mark.parametrize("url,expected", [
    ('name', 'name'),
    (None, None),
    ('http://example.com:123/p?q=1#f', 'http://example.com:123/p?q=1#f'),
    ('http://user@example.com/', 'http://example.com/'),
    ('http://user:pwd@example.com/', 'http://example.com/'),
])
def test_sanitize_url(url, expected):
    assert sanitize_url(url) == expected


def test_url_without_fragment():
    assert url_without_fragment('http://example.org/p#frag#ment') == 'http://example.org/p'
