# coding: utf8
from __future__ import unicode_literals, print_function, division
from unittest import TestCase

from mock import Mock
from clldutils.testing import WithTempDir
from clldutils.csvw.metadata import TableGroup, ForeignKey, URITemplate, Column
from clldutils.path import copy

from pycldf.tests.util import FIXTURES
from pycldf.terms import term_uri


class TestMakeColumn(TestCase):
    def test_make_column(self):
        from pycldf.dataset import make_column

        self.assertEqual(make_column('name').datatype.base, 'string')
        self.assertEqual(
            make_column({'name': 'num', 'datatype': 'decimal'}).datatype.base, 'decimal')
        self.assertEqual(make_column(term_uri('latitude')).datatype.base, 'decimal')
        self.assertEqual(make_column(term_uri('source')).separator, ';')
        self.assertIsNone(make_column(Column('name')).datatype)
        with self.assertRaises(TypeError):
            make_column(5)


class TestGeneric(WithTempDir):
    def _make_one(self):
        from pycldf.dataset import Generic

        return Generic.in_dir(self.tmp_path())

    def test_primary_table(self):
        ds = self._make_one()
        self.assertIsNone(ds.primary_table)

    def test_column_access(self):
        ds = self._make_one()
        with self.assertRaises(KeyError):
            assert ds['']

        ds.add_component('ValueTable')
        self.assertEqual(ds['ValueTable'], ds['values.csv'])

        with self.assertRaises(KeyError):
            assert ds['ValueTable', 'colx']

        self.assertEqual(
            ds['ValueTable', 'Language_ID'], ds['values.csv', 'languageReference'])

    def test_foreign_key_creation(self):
        ds = self._make_one()
        ds.add_component('ValueTable')
        self.assertEqual(ds['ValueTable'].tableSchema.foreignKeys, [])
        ds.add_component('LanguageTable')
        self.assertEqual(len(ds['ValueTable'].tableSchema.foreignKeys), 1)
        ds.write(
            ValueTable=[{
                'ID': '1',
                'Language_ID': 'abc',
                'Parameter_ID': 'xyz',
                'Value': '?',
            }],
            LanguageTable=[])
        with self.assertRaises(ValueError):
            ds.validate()
        ds.write(
            ValueTable=[{
                'ID': '1',
                'Language_ID': 'abc',
                'Parameter_ID': 'xyz',
                'Value': '?',
            }],
            LanguageTable=[{'ID': 'abc', 'Name': 'language'}])
        ds.validate()

    def test_add_table(self):
        ds = self._make_one()
        ds.add_table('stuff.csv', term_uri('id'), 'col1')
        ds.write(fname=self.tmp_path('t.json'), **{'stuff.csv': [{'ID': '.ab'}]})
        with self.assertRaises(ValueError):
            ds.validate()
        ds['stuff.csv', 'ID'].name = 'nid'
        with self.assertRaises(ValueError):
            ds.add_columns('stuff.csv', term_uri('id'))
        with self.assertRaises(ValueError):
            ds.add_columns('stuff.csv', 'col1')

    def test_add_foreign_key(self):
        ds = self._make_one()
        ds.add_table('primary.csv', term_uri('id'), 'col1')
        ds.add_table('foreign.csv', 'fk_id')
        ds.write(**{'primary.csv': [{'ID': 'ab'}], 'foreign.csv': [{'fk_id': 'xy'}]})
        ds.validate()

        ds.add_foreign_key('foreign.csv', 'fk_id', 'primary.csv')
        ds.write(**{'primary.csv': [{'ID': 'ab'}], 'foreign.csv': [{'fk_id': 'xy'}]})
        with self.assertRaises(ValueError):
            ds.validate()

        ds.add_foreign_key('foreign.csv', 'fk_id', 'primary.csv', 'ID')
        ds.write(**{'primary.csv': [{'ID': 'ab'}], 'foreign.csv': [{'fk_id': 'xy'}]})
        with self.assertRaises(ValueError):
            ds.validate()


class TestWordlist(WithTempDir):
    def test_cognates(self):
        from pycldf.dataset import Wordlist

        ds = Wordlist.in_dir(self.tmp_path())
        ds['FormTable', 'Segments'].separator = None
        ds.write(
            FormTable=[
                {'ID': '1',
                 'Value': 'form',
                 'Form': 'abcdefg',
                 'Segments': 'a bc d e f',
                 'Language_ID': 'l',
                 'Parameter_ID': 'p'}
            ],
        )
        self.assertEqual(
            ' '.join(ds.get_soundsequence(list(ds['FormTable'])[0])), 'a bc d e f')

    def test_partial_cognates(self):
        from pycldf.dataset import Wordlist

        ds = Wordlist.in_dir(self.tmp_path())
        ds['FormTable'].get_column('Segments').separator = '+'
        ds.add_component('PartialCognateTable')
        ds.write(
            FormTable=[
                {'ID': '1',
                 'Value': 'form',
                 'Form': 'abcdefg',
                 'Segments': ['a bc', 'd e f', 'g'],
                 'Language_ID': 'l',
                 'Parameter_ID': 'p'}
            ],
            PartialCognateTable=[
                {
                    'ID': '1',
                    'Form_ID': '1',
                    'Cognateset_ID': '1',
                    'Slice': ['1:3'],
                }
            ],
        )
        self.assertEqual(
            ' '.join(ds.get_soundsequence(list(ds['FormTable'])[0])),
            'a bc d e f g')
        self.assertEqual(
            ' '.join(ds.get_subsequence(list(ds['PartialCognateTable'])[0])),
            'd e f g')


class Tests(WithTempDir):
    def _make_tg(self, *tables):
        tg = TableGroup.fromvalue({'tables': list(tables)})
        tg._fname = self.tmp_path('md.json')
        return tg

    def test_add_component(self):
        from pycldf.dataset import Wordlist

        ds = Wordlist.in_dir(self.tmp_path())
        ds['FormTable'].tableSchema.foreignKeys.append(ForeignKey.fromdict({
            'columnReference': 'Language_ID',
            'reference': {'resource': 'languages.csv', 'columnReference': 'ID'}}))
        ds.add_component('LanguageTable')
        with self.assertRaises(ValueError):
            ds.add_component('LanguageTable')
        ds.add_component('ParameterTable', {'name': 'url', 'datatype': 'anyURI'})

        ds.write(
            FormTable=[
                {'ID': '1', 'Form': 'form', 'Language_ID': 'l', 'Parameter_ID': 'p'}],
            LanguageTable=[{'ID': 'l'}],
            ParameterTable=[{'ID': 'p'}])
        ds.validate()

        ds.write(
            FormTable=[
                {'ID': '1', 'Value': 'form', 'Language_ID': 'l', 'Parameter_ID': 'x'}],
            LanguageTable=[{'ID': 'l'}],
            ParameterTable=[{'ID': 'p'}])
        with self.assertRaises(ValueError):
            ds.validate()

    def test_modules(self):
        from pycldf.dataset import Dataset, Wordlist, Dictionary, StructureDataset

        ds = Dataset(self._make_tg())
        self.assertIsNone(ds.primary_table)
        ds = Dataset(self._make_tg({"url": "data.csv"}))
        self.assertIsNone(ds.primary_table)
        ds = Dataset(self._make_tg({
            "url": "data.csv",
            "dc:conformsTo": "http://cldf.clld.org/v1.0/terms.rdf#ValueTable"}))
        self.assertEqual(ds.primary_table, 'ValueTable')
        self.assertIsNotNone(Wordlist.in_dir(self.tmp_path()).primary_table)
        self.assertIsNotNone(Dictionary.in_dir(self.tmp_path()).primary_table)
        self.assertIsNotNone(StructureDataset.in_dir(self.tmp_path()).primary_table)

    def test_Dataset_from_scratch(self):
        from pycldf.dataset import Dataset

        copy(FIXTURES.joinpath('ds1.csv'), self.tmp_path('xyz.csv'))
        with self.assertRaises(ValueError):
            Dataset.from_data(self.tmp_path('xyz.csv'))

        copy(FIXTURES.joinpath('ds1.csv'), self.tmp_path('values.csv'))
        ds = Dataset.from_data(self.tmp_path('values.csv'))
        self.assertEqual(ds.module, 'StructureDataset')

        self.assertEqual(len(list(ds['ValueTable'])), 2)
        ds.validate()
        ds['ValueTable'].write(2 * list(ds['ValueTable']))
        with self.assertRaises(ValueError):
            ds.validate()
        md = ds.write_metadata()
        Dataset.from_metadata(md)
        repr(ds)
        del ds._tg.common_props['dc:conformsTo']
        Dataset.from_metadata(ds.write_metadata())
        self.assertEqual(len(ds.stats()), 1)

    def test_Dataset_validate(self):
        from pycldf.dataset import StructureDataset

        ds = StructureDataset.in_dir(self.tmp_path('new'))
        ds.write(ValueTable=[])
        ds.validate()
        ds['ValueTable'].tableSchema.columns = []
        with self.assertRaises(ValueError):
            ds.validate()
        ds._tg.tables = []
        with self.assertRaises(ValueError):
            ds.validate()

        ds = StructureDataset.in_dir(self.tmp_path('new'))
        ds.add_component('LanguageTable')
        ds.write(ValueTable=[])
        ds['LanguageTable'].common_props['dc:conformsTo'] = 'http://cldf.clld.org/404'
        with self.assertRaises(ValueError):
            ds.validate()

        ds = StructureDataset.in_dir(self.tmp_path('new'))
        ds['ValueTable'].get_column('Source').propertyUrl = URITemplate(
            'http://cldf.clld.org/404')
        ds.write(ValueTable=[])
        with self.assertRaises(ValueError):
            ds.validate()

    def test_Dataset_write(self):
        from pycldf.dataset import StructureDataset

        ds = StructureDataset.from_metadata(self.tmp)
        ds.write(ValueTable=[])
        self.assertTrue(self.tmp_path('values.csv').exists())
        ds.validate()
        ds.add_sources("@misc{ky,\ntitle={the title}\n}")
        ds.write(ValueTable=[
            {
                'ID': '1',
                'Language_ID': 'abcd1234',
                'Parameter_ID': 'f1',
                'Value': 'yes',
                'Source': ['key[1-20]', 'ky'],
            }])
        with self.assertRaises(ValueError):
            ds.validate()
        ds.sources.add("@misc{key,\ntitle={the title}\n}")
        ds.write(ValueTable=[
            {
                'ID': '1',
                'Language_ID': 'abcd1234',
                'Parameter_ID': 'f1',
                'Value': 'yes',
                'Source': ['key[1-20]'],
            }])
        ds.validate()
        ds.add_component('ExampleTable')
        ds.write(
            ValueTable=[
                {
                    'ID': '1',
                    'Language_ID': 'abcd1234',
                    'Parameter_ID': 'f1',
                    'Value': 'yes',
                    'Source': ['key[1-20]'],
                }],
            ExampleTable=[
                {
                    'ID': '1',
                    'Language_ID': 'abcd1234',
                    'Primary': 'si',
                    'Translation': 'yes',
                    'Analyzed': ['morph1', 'morph2', 'morph3'],
                    'Gloss': ['gl1', 'gl2'],
                }])
        with self.assertRaises(ValueError):
            ds.validate()
        ds['ExampleTable'].write([
            {
                'ID': '1',
                'Language_ID': 'abcd1234',
                'Primary': 'si',
                'Translation': 'yes',
                'Analyzed': ['morph1', 'morph2', 'morph3'],
                'Gloss': ['gl1', 'gl2', 'gl3'],
            }])
        ds.validate()

    def test_validators(self):
        from pycldf.dataset import Dataset

        copy(FIXTURES.joinpath('invalid.csv'), self.tmp_path('values.csv'))
        ds = Dataset.from_data(self.tmp_path('values.csv'))

        with self.assertRaises(ValueError):
            ds.validate()

        log = Mock()
        ds.validate(log=log)
        self.assertEqual(log.warn.call_count, 2)

        for col in ds._tg.tables[0].tableSchema.columns:
            if col.name == 'Language_ID':
                col.propertyUrl.uri = 'http://cldf.clld.org/v1.0/terms.rdf#glottocode'

        log = Mock()
        ds.validate(log=log)
        self.assertEqual(log.warn.call_count, 4)

    def test_Module(self):
        from pycldf.dataset import get_modules

        self.assertFalse(get_modules()[0].match(5))
