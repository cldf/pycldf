import re
import functools


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
    gloss, morphemes = row[column.name], None
    col = table.get_column('http://cldf.clld.org/v1.0/terms.rdf#analyzedWord')
    if col:
        morphemes = row[col.name]

    if gloss and morphemes and len(gloss) != len(morphemes):
        raise ValueError('number of morphemes and glosses does not match')


VALIDATORS = [
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
        valid_references)
]
