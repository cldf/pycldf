# coding: utf8
from __future__ import unicode_literals, print_function, division

from mock import MagicMock
from clldutils.clilib import ParserError
from clldutils.testing import WithTempDir, capture

from pycldf.tests.util import FIXTURES, make_dataset


class Tests(WithTempDir):
    def test_datasets(self):
        from pycldf.cli import datasets

        with self.assertRaises(ParserError):
            datasets(MagicMock(args=MagicMock()))

        with self.assertRaises(ParserError):
            datasets(MagicMock(args=[self.tmp_path('new').as_posix()]))

        with capture(datasets, MagicMock(args=[FIXTURES.as_posix(), "dc:title"])) as out:
            self.assertIn('a cldf dataset', out)

    def test_stats(self):
        from pycldf.cli import stats

        with self.assertRaises(ParserError):
            stats(MagicMock(args=MagicMock()))

        with self.assertRaises(ParserError):
            stats(MagicMock(args=[self.tmp_path('new').as_posix()]))

        ds = make_dataset(rows=[[1, 1, 1, 1]])
        ds.write(self.tmp_path(), archive=True)
        with capture(stats,
                     MagicMock(args=[self.tmp_path('test.zip').as_posix()])) as out:
            self.assertIn('languages: 1', out)

        with capture(stats,
                     MagicMock(args=[
                         FIXTURES.joinpath('ds1.csv-metadata.json').as_posix()])) as out:
            self.assertIn('languages: 1', out)

        with capture(stats,
                     MagicMock(args=[FIXTURES.joinpath('ds1.csv').as_posix()])) as out:
            self.assertIn('languages: 1', out)
