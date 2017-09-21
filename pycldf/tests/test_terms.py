from __future__ import unicode_literals
from unittest import TestCase


class Tests(TestCase):
    def test_Terms(self):
        from pycldf.terms import TERMS

        self.assertIn('alignment', TERMS.properties)

        with self.assertRaises(ValueError):
            TERMS.is_cldf_uri('http://cldf.clld.org/404')
        self.assertFalse(TERMS.is_cldf_uri('http://example.org'))
        self.assertTrue(TERMS.is_cldf_uri('http://cldf.clld.org/v1.0/terms.rdf#source'))
