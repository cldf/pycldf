from __future__ import unicode_literals
from unittest import TestCase


class Tests(TestCase):
    def test_Terms(self):
        from pycldf.terms import TERMS

        self.assertIn('alignment', TERMS.properties)
