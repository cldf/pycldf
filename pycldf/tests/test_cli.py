# coding: utf8
from __future__ import unicode_literals, print_function, division

from mock import MagicMock
from clldutils.clilib import ParserError
from clldutils.testing import WithTempDir, capture

from pycldf.tests.util import FIXTURES


class Tests(WithTempDir):
    def test_datasets(self):
        from pycldf.cli import datasets

        with self.assertRaises(ParserError):
            datasets(MagicMock(args=MagicMock()))

        with self.assertRaises(ParserError):
            datasets(MagicMock(args=[self.tmp_path('new').as_posix()]))

        with capture(datasets, MagicMock(args=[FIXTURES.as_posix(), "dc:title"])) as out:
            self.assertIn('a cldf dataset', out)
