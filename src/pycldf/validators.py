# coding: utf8
from __future__ import unicode_literals, print_function, division
import re
from functools import partial


def valid_references(dataset, table, column, row):
    if dataset.sources:
        dataset.sources.validate(row[column.name])


def valid_regex(pattern, name, dataset, table, column, row):
    value = row[column.name]
    if value is not None and not pattern.match(value):
        raise ValueError('invalid {0}: {1}'.format(name, value))


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
        partial(valid_regex, re.compile(r'[a-z]{3}$'), 'ISO 639-3 code')),
    (
        None,
        'http://cldf.clld.org/v1.0/terms.rdf#glottocode',
        partial(valid_regex, re.compile(r'[a-z0-9]{4}[0-9]{4}$'), 'glottocode')),
    (
        None,
        'http://cldf.clld.org/v1.0/terms.rdf#gloss',
        valid_igt),
    (
        None,
        'http://cldf.clld.org/v1.0/terms.rdf#source',
        valid_references)
]
