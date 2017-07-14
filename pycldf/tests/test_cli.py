# coding: utf8
from __future__ import unicode_literals, print_function, division

from mock import MagicMock
from clldutils.clilib import ParserError
from clldutils.testing import WithTempDir, capture
from clldutils.path import copy

from pycldf.tests.util import FIXTURES


class Tests(WithTempDir):
    def test_stats(self):
        from pycldf.cli import stats

        with self.assertRaises(ParserError):
            stats(MagicMock(args=MagicMock()))

        with self.assertRaises(ParserError):
            stats(MagicMock(args=[self.tmp_path('new').as_posix()]))

    def test_all(self):
        from pycldf.cli import validate, stats

        md = self.tmp_path('md.json')
        copy(FIXTURES.joinpath('ds1.csv-metadata.json'), md)
        copy(FIXTURES.joinpath('ds1.bib'), self.tmp_path('ds1.bib'))
        copy(FIXTURES.joinpath('ds1.csv'), self.tmp_path('ds1.csv'))

        with capture(validate, MagicMock(args=[md.as_posix()])) as out:
            self.assertEqual(out, '')

        with capture(stats, MagicMock(args=[md.as_posix()])) as out:
            self.assertIn('cldf:v1.0:StructureDataset', out)
