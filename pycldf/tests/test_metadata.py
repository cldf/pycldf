# coding: utf8
from __future__ import unicode_literals, print_function, division
import datetime

from clldutils.testing import WithTempDir
from clldutils.path import Path


FIXTURES = Path(__file__).parent.joinpath('fixtures')


class Tests(WithTempDir):
    def test_DictWrapper(self):
        from pycldf.metadata import DictWrapper

        orig = {}
        d = DictWrapper(orig)
        d['a'] = 1
        assert 'a' in d and 'a' in orig
        assert d['a']

        for attr in ['keys', 'items', 'values']:
            self.assertEqual(list(getattr(orig, attr)()), list(getattr(d, attr)()))

        for _ in d:
            pass

        del d['a']
        assert 'a' not in d and 'a' not in orig

    def test_Table(self):
        from pycldf.metadata import Table, Schema

        t = Table({'url': 'a', 'tableSchema': {}})
        self.assertEqual(t.url, 'a')
        self.assertIsInstance(t.schema, Schema)
        t.schema.aboutUrl = 'abc'
        self.assertEqual(t['tableSchema']['aboutUrl'], 'abc')

    def test_Schema(self):
        from pycldf.metadata import Schema

        s = Schema({'primaryKey': 'a', 'columns': []})
        self.assertEqual(s.primaryKey, 'a')
        with self.assertRaises(ValueError):
            s.primaryKey = 'a'

    def test_converters(self):
        from pycldf.metadata import boolean, parse_date, parse_datetime, parse_json

        self.assertTrue(boolean(True))
        self.assertTrue(boolean('yes'))
        self.assertFalse(boolean('n'))
        self.assertRaises(ValueError, boolean, 'x')

        d = datetime.datetime.now()
        self.assertEqual(d, parse_datetime(d))
        self.assertEqual(d.date(), parse_date(d))
        self.assertEqual(d.date(), parse_date(d.date()))

        self.assertEqual({}, parse_json({}))

    def test_type_conversion(self):
        from pycldf.metadata import Column

        col = Column({'datatype': 'string'})
        self.assertEqual(col.unmarshal(''), None)
        self.assertEqual(col.marshal(None), '')

        for name, value in [
            ('integer', 1),
            ('decimal', 2.456),
            ('float', 5.45),
            ('boolean', False),
            ('date', datetime.date.today()),
            ('datetime', datetime.datetime.now()),
            ('json', {'a': 2, 'b': False}),
        ]:
            col = Column({'datatype': name, 'name': name})
            col.valueUrl = 'abc'
            self.assertEqual(col.name, name)
            assertion = self.assertAlmostEqual \
                if isinstance(value, float) else self.assertEqual
            assertion(value, col.unmarshal(col.marshal(value)))
