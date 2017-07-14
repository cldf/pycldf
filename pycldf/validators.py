# coding: utf8
from __future__ import unicode_literals, print_function, division
import re
from functools import partial


#
# FIXME: add a validator for `Language_ID` in case there's a languages.csv!
#


def valid_references(dataset, table, column, row):
    if dataset.sources:
        dataset.sources.validate(row[column.name])


def valid_regex(regex, name, dataset, table, column, row):
    value = row[column.name]
    if value is not None and not re.match(regex, value):
        raise ValueError('invalid {0}: {1}'.format(name, value))


def valid_igt(dataset, table, column, row):
    gloss, morphemes = row[column.name], None
    for col in table.tableSchema.columns:
        if col.propertyUrl and \
                col.propertyUrl.uri == 'http://purl.org/linguistics/gold/Morpheme':
            morphemes = row[col.name]
            break

    if gloss and morphemes and len(gloss) != len(morphemes):
        raise ValueError('number of morphemes and glosses does not match')


VALIDATORS = {
    'http://cldf.clld.org/terms.rdf#iso639P3code':
        partial(valid_regex, '[a-z]{3}$', 'ISO 639-3 code'),
    'http://cldf.clld.org/terms.rdf#glottocode':
        partial(valid_regex, '[a-z0-9]{4}[0-9]{4}$', 'glottocode'),
    'http://purl.org/linguistics/gold/hasGlosses': valid_igt,
    'http://purl.org/dc/terms/source': valid_references,
}
