# coding: utf8
from __future__ import unicode_literals

import pytest

from pycldf.sources import Sources, Source, Reference

BIB = """@BOOK{Obrazy,
    author = {Borovský, Karel Havlíček},
    title = {Obrazy z Rus}
}

@BOOK{Elegie,
    author = {Borovský, Karel Havlíček},
    title = {Tirolské elegie}
}
"""


def test_Sources(tmpdir):
    src = Sources()
    src.add(BIB, Source(
        'book', 'huber2005', author='Herrmann Huber', year='2005', title='y'))
    for entry in src:
        assert entry.genre == 'book'
        break
    assert len(list(src.items())) == 3
    assert len(list(src.keys())) == 3
    refs = ['huber2005[1-6]', 'Obrazy', 'Elegie[34]']
    assert src.format_refs(*list(src.expand_refs(refs))) == refs
    assert '%s' % src['huber2005'] == 'Huber, Herrmann. 2005. y.'
    with pytest.raises(ValueError):
        src.add(5)
    with pytest.raises(ValueError):
        src.add('@misc{a.b,\n  author="a.b"\n}')
    with pytest.raises(ValueError):
        _ = src['unknown']
        assert _  # pragma: no cover
    with pytest.raises(ValueError):
        src.parse('a[x')
    with pytest.raises(ValueError):
        src.parse('[x]')
    with pytest.raises(ValueError):
        src.validate(['x'])

    bib = str(tmpdir / 'test.bib')
    src.write(bib)

    src2 = Sources()
    src2.read(bib)

    src2.write(bib, ids=['huber2005'])
    src = Sources.from_file(bib)
    assert len(src) == 1


def test_Source_from_bibtex():
    bibtex = '@' + BIB.split('@')[1]
    assert Source.from_bibtex(bibtex).entry.fields['title'] == 'Obrazy z Rus'


def test_Sources_with_None_values(tmpdir):
    src = Sources()
    src.add(Source('book', 'huber2005', title=None))
    bib = tmpdir / 'test.bib'
    src.write(str(bib))


@pytest.mark.parametrize(
    "bibtex,expected",
    [
        ('@book{1,\ntitle={Something about \\& and \\_}}', 'Something about \\& and \\_'),
        ('@book{1,\ntitle={Something about & and _}}', 'Something about & and _'),
    ]
)
def test_Sources_roundtrip_latex(tmpdir, bibtex, expected):
    src = Sources()
    src.add(bibtex)
    bib = tmpdir / 'test.bib'
    src.write(bib)
    assert expected in bib.read_text('utf8')


def test_Source_expand_refs():
    sources = Sources()
    src = Source(
        'book', 'Meier2005', author='Hans Meier', year='2005', title='The Book')
    assert 'Meier2005' in repr(src)
    sources.add(src)
    bib = sources._bibdata.to_string(bib_format='bibtex')
    assert len(bib.split('author')) == 2
    assert len(list(sources.expand_refs('Meier2005'))) == 1
    bib = sources._bibdata.to_string(bib_format='bibtex')
    assert len(bib.split('author')) == 2
    assert len(list(sources.expand_refs('12345'))) == 1


def test_Reference():
    ref = Reference(Source('book', 'huber2005', author='Herrmann Huber'), '2-5')
    assert '2-5' in repr(ref)
    assert '%s' % ref == 'huber2005[2-5]'
    with pytest.raises(ValueError):
        Reference(Source('book', 'huber2005', author='Herrmann Huber'), '[2-5]')


def test_Source_persons():
    assert len(list(Source.persons('A. Meier'))) == 1
    assert len(list(Source.persons('Meier, A.B.'))) == 1
    assert len(list(Source.persons('A. Meier, B. Meier, C.Meier'))) == 3
