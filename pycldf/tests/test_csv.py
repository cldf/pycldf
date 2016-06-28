# coding: utf8
from __future__ import unicode_literals, print_function, division

from clldutils.jsonlib import load
from clldutils.testing import WithTempDir

from pycldf.tests.util import FIXTURES
from pycldf.util import Archive
from pycldf.metadata import Table


class Tests(WithTempDir):
    def test_read_write(self):
        from pycldf.csv import Reader, Writer

        table = load(FIXTURES.joinpath('ds1.csv-metadata.json'))['tables'][0]
        table['tableSchema']['columns'][0]['datatype'] = 'integer'
        table['url'] = 'test.tsv'

        row = '1,abcd1234,fid1,yes,,80086;meier2015[2-5]'.split(',')

        with Archive(self.tmp_path('test.zip'), 'w') as archive:
            with Writer(table, container=archive) as writer:
                writer.writerow(row)

        with Archive(self.tmp_path('test.zip')) as archive:
            with Reader(table, container=archive) as reader:
                rows = list(reader)
                self.assertEqual(rows[0]['ID'], 1)
                self.assertEqual(
                    rows[0].valueUrl('Language_ID'),
                    'http://glottolog.org/resource/languoid/id/abcd1234')
                self.assertEqual(rows[0].to_list(), row)

        table = Table(table)
        del table.dialect['header']
        self.assertTrue(table.dialect.header)
        del table.dialect['delimiter']
        self.assertEqual(table.dialect.delimiter, ',')
        table.dialect.header = False

        with Writer(table, container=self.tmp_path()) as writer:
            writer.writerow(row)
            writer.writerows(rows)

        with Reader(table, container=self.tmp_path()) as reader:
            rows = list(reader)
            self.assertEqual(rows[0]['ID'], 1)
            self.assertEqual(
                rows[0].valueUrl('Language_ID'),
                'http://glottolog.org/resource/languoid/id/abcd1234')
            self.assertEqual(rows[0].to_list(), row)
