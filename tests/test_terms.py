import pytest


def test_terms():
    from pycldf.terms import TERMS

    assert 'alignment' in TERMS.properties

    with pytest.raises(ValueError):
        TERMS.is_cldf_uri('http://cldf.clld.org/404')

    assert not TERMS.is_cldf_uri('http://example.org')
    assert TERMS.is_cldf_uri('http://cldf.clld.org/v1.0/terms.rdf#source')

    assert len(TERMS.properties) + len(TERMS.classes) == len(TERMS)
    assert len(TERMS.modules) + len(TERMS.components) == len(TERMS.classes)

    assert 'LanguageTable' in TERMS.components
    assert 'LanguageTable' not in TERMS.modules
    assert 'Wordlist' in TERMS.modules
