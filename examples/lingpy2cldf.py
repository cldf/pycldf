# coding: utf8
from __future__ import unicode_literals, print_function, division

from clldutils.path import Path, read_text
from clldutils.dsv import reader, Dialect
from clldutils.misc import slug
from pycldf import Wordlist

LINGPY_DIALECT = Dialect(delimiter='\t', skipBlankRows=True)


def main(out):
    ds = Wordlist.in_dir(out)
    ds.add_sources(read_text('lingpy_tutorial/polynesian.bib'))
    ds.add_component('LanguageTable')
    ds.add_component('ParameterTable', 'Concepticon_ID')
    ds.add_component('CognateTable')

    languages, parameters, forms, cognates = {}, {}, [], []

    for d in reader('lingpy_tutorial/polynesian.tsv', dicts=True, dialect=LINGPY_DIALECT):
        lid = slug(d['DOCULECT'])
        if lid not in languages:
            languages[lid] = dict(
                ID=lid,
                Name=d['DOCULECT_IN_SOURCE'],
                Glottocode=d['GLOTTOCODE'])

        pid = d['CONCEPTICON_ID']
        if pid not in parameters:
            parameters[pid] = dict(
                ID=pid,
                Name=d['CONCEPT'],
                Concepticon_ID=pid)

        forms.append(dict(
            ID=d['ID'],
            Language_ID=lid,
            Parameter_ID=pid,
            Form=d['FORM'],
            Segments=d['TOKENS'].split(),
            Source=[d['SOURCE']] if d['SOURCE'] else []))
        cognates.append(dict(ID=d['ID'], Form_ID=d['ID'], Cognateset_ID=d['COGID']))

    ds.write(
        FormTable=forms,
        LanguageTable=languages.values(),
        ParameterTable=parameters.values(),
        CognateTable=cognates)


if __name__ == '__main__':
    out = Path('lingpy_cldf')
    if not out.exists():
        out.mkdir()
    main(out)
