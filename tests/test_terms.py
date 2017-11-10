from __future__ import unicode_literals

import pytest


def test_terms():
    from pycldf.terms import TERMS

    assert 'alignment' in TERMS.properties

    with pytest.raises(ValueError):
        TERMS.is_cldf_uri('http://cldf.clld.org/404')

    assert not TERMS.is_cldf_uri('http://example.org')
    assert TERMS.is_cldf_uri('http://cldf.clld.org/v1.0/terms.rdf#source')
