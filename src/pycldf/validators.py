import re
import warnings
import functools
from typing import Optional, Callable, TYPE_CHECKING
import logging
import dataclasses

from clldutils.misc import log_or_raise

from pycldf.terms import Terms
from pycldf.util import iter_uritemplates

if TYPE_CHECKING:  # pragma: no cover
    from pycldf.dataset import Dataset, Table, RowType, Column

RowValidatorType = Callable[['Dataset', 'Table', 'Column', 'RowType'], None]


@dataclasses.dataclass
class DatasetValidator:
    dataset: 'Dataset'
    success: bool
    terms: Terms
    log: Optional[logging.Logger]
    row_validators: list[tuple[Optional[str], str, RowValidatorType]]

    def __post_init__(self):
        self.row_validators.extend(VALIDATORS)

    def fail(self, reason):
        self.success = False
        log_or_raise(reason, log=self.log)

    def warn(self, msg, *args):
        if self.log:
            self.log.warning(msg, *args)

    def validate_rows(self, table):
        # FIXME: see if table.common_props['dc:conformsTo'] is in validators!  pylint: disable=W0511
        validators = []
        for col in table.tableSchema.columns:
            for table_, col_, v_ in self.row_validators:
                if ((not table_ or table is self.dataset.get(table_))
                        and col is self.dataset.get((table, col_))):  # noqa: W503
                    validators.append((col, v_))

        for fname, lineno, row in table.iterdicts(log=self.log, with_metadata=True):
            for col, validate in validators:
                try:
                    validate(self.dataset, table, col, row)
                except ValueError as e:
                    self.fail(f'{fname.name}:{lineno}:{col.name} {e}')

    def validate_columns(self, table):
        property_urls, colnames = set(), set()
        for col in table.tableSchema.columns:
            if col.header in colnames:  # pragma: no cover
                self.fail(f'Duplicate column name in table schema: {table.url} {col.header}')
            colnames.add(col.header)
            if col.propertyUrl:
                col_uri = col.propertyUrl.uri
                try:
                    self.terms.is_cldf_uri(col_uri)
                    if col_uri in property_urls:  # pragma: no cover
                        self.fail(
                            f'Duplicate CLDF property in table schema: {table.url} {col_uri}')
                    property_urls.add(col_uri)
                except ValueError:
                    self.fail(f'invalid CLDF URI: {col_uri}')

    def validate_table_schema(self, table):
        tmpl_vars = set(col.name for col in table.tableSchema.columns)
        for obj, prop, tmpl in iter_uritemplates(table):
            if not {n for n in tmpl.variable_names if not n.startswith('_')}.issubset(tmpl_vars):
                self.warn(f'Unknown variables in URI template: {obj}:{prop}:{tmpl}')

        type_uri = table.common_props.get('dc:conformsTo')
        if type_uri:
            try:
                self.terms.is_cldf_uri(type_uri)
            except ValueError:
                self.fail(f'invalid CLDF URI: {type_uri}')

        if not table.tableSchema.primaryKey:
            self.warn(
                'Table without primary key: %s - %s',
                table.url,
                'This may cause problems with "cldf createdb"')
        elif len(table.tableSchema.primaryKey) > 1:
            self.warn(
                'Table with composite primary key: %s - %s',
                table.url,
                'This may cause problems with "cldf createdb"')

    def validate_default_objects(self, default_table):
        dtable_uri = default_table.common_props['dc:conformsTo']
        try:
            table = self.dataset[dtable_uri]
        except KeyError:
            self.fail(f'{self.dataset.module} requires {dtable_uri}')
            return

        default_cols = {c.propertyUrl.uri: c for c in default_table.tableSchema.columns}
        required_default_cols = {
            c.propertyUrl.uri for c in default_table.tableSchema.columns
            if c.required or c.common_props.get('dc:isRequiredBy')}
        cols = {
            c.propertyUrl.uri: c for c in table.tableSchema.columns
            if c.propertyUrl}
        table_uri = table.common_props['dc:conformsTo']
        for col in required_default_cols - set(cols.keys()):
            self.fail(f'{table_uri} requires column {col}')
        for uri, col in cols.items():
            default = default_cols.get(uri)
            if default:
                cardinality = default.common_props.get('dc:extent')
                if not cardinality:
                    cardinality = self.terms.by_uri[uri].cardinality
                if (cardinality == 'multivalued' and not col.separator) or \
                        (cardinality == 'singlevalued' and col.separator):
                    self.fail(f'{table_uri} {uri} must be {cardinality}')


def valid_references(dataset, table, column, row):
    if dataset.sources:
        dataset.sources.validate(row[column.name])


def valid_regex(pattern, name, dataset, table, column, row):
    value = row[column.name]
    if value is not None:
        if not isinstance(value, list):
            # Normalize to also work with list-valued columns:
            value = [value]
        for val in value:
            if not pattern.match(val):
                raise ValueError('invalid {0}: {1} (in {2})'.format(name, val, value))


def valid_igt(dataset, table, column, row):
    word_glosses, words = row[column.name], None
    col = table.get_column('http://cldf.clld.org/v1.0/terms.rdf#analyzedWord')
    if col:
        words = row[col.name]

    if word_glosses and words and len(word_glosses) != len(words):
        raise ValueError('number of words and word glosses does not match')


def valid_grammaticalityJudgement(dataset, table, column, row):
    lid_name = dataset.readonly_column_names.ExampleTable.languageReference[0]
    gc_name = dataset.readonly_column_names.LanguageTable.glottocode[0]
    if row[column.name] is not None:
        lg = dataset.get_row('LanguageTable', row[lid_name])
        if lg[gc_name]:
            raise ValueError('Glottolog language linked from ungrammatical example')


def valid_mediaType(dataset, table, column, row):
    main, _, sub = row[column.name].partition('/')
    if not re.fullmatch('[a-z]+', main):
        warnings.warn('Invalid main part in media type: {}'.format(main))


VALIDATORS: list[tuple[None, str, RowValidatorType]] = [
    (
        None,
        'http://cldf.clld.org/v1.0/terms.rdf#iso639P3code',
        functools.partial(valid_regex, re.compile(r'[a-z]{3}$'), 'ISO 639-3 code')),
    (
        None,
        'http://cldf.clld.org/v1.0/terms.rdf#glottocode',
        functools.partial(valid_regex, re.compile(r'[a-z0-9]{4}[0-9]{4}$'), 'glottocode')),
    (
        None,
        'http://cldf.clld.org/v1.0/terms.rdf#gloss',
        valid_igt),
    (
        None,
        'http://cldf.clld.org/v1.0/terms.rdf#source',
        valid_references),
    (
        None,
        'http://cldf.clld.org/v1.0/terms.rdf#grammaticalityJudgement',
        valid_grammaticalityJudgement),
    (
        None,
        'http://cldf.clld.org/v1.0/terms.rdf#mediaType',
        valid_mediaType),
]
