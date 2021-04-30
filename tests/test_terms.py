import pytest


def test_terms():
    from pycldf.terms import TERMS

    assert 'alignment' in TERMS.properties

    assert not TERMS.is_cldf_uri('http://example.org')
    assert TERMS.is_cldf_uri('http://cldf.clld.org/v1.0/terms.rdf#source')

    assert len(TERMS.properties) + len(TERMS.classes) == len(TERMS)
    assert len(TERMS.modules) + len(TERMS.components) == len(TERMS.classes)

    assert 'LanguageTable' in TERMS.components
    assert 'LanguageTable' not in TERMS.modules
    assert 'Wordlist' in TERMS.modules
    id_ = TERMS['id']
    assert id_.version == 'v1.0'
    assert '<p>' in id_.comment()
    assert '</a>' in TERMS['Wordlist'].comment(one_line=True)
    assert TERMS['languageReference'].cardinality is None


def test_invalid_uri():
    from pycldf.terms import TERMS

    with pytest.warns(UserWarning):
        with pytest.raises(ValueError):
            TERMS.is_cldf_uri('http://cldf.clld.org/unknown')


def test_cltsReference():
    from pycldf.terms import TERMS

    col = TERMS['cltsReference'].to_column()
    assert col.datatype.read('NA') and col.datatype.read('rounded_open-mid_central_vowel')
    with pytest.raises(ValueError):
        col.datatype.read('Na')
