from __future__ import unicode_literals
from collections import defaultdict
from itertools import groupby

from sqlalchemy import create_engine

from clldutils.path import Path
from pycldf import Wordlist

SQL_FORMS = """\
SELECT 
  l.pk, l.id, l.name, vs.pk, v.id, p.id, u.name, u.pk
FROM 
  counterpart as cp,
  value AS v, 
  valueset AS vs, 
  language AS l,
  parameter AS p, 
  contribution AS c,
  unit AS u
WHERE
  cp.word_pk = u.pk
  AND cp.pk = v.pk
  AND v.valueset_pk = vs.pk 
  AND vs.contribution_pk = c.pk
  AND vs.language_pk = l.pk 
  AND vs.parameter_pk = p.pk 
  AND c.id = '{0}' 
ORDER BY
  p.pk, u.name"""

SQL_SOURCE_FORMS = """\
SELECT 
  l.pk, l.id, l.name, u.name, u.id, lo.target_word_pk, lo.relation, lo.certain
FROM 
  language AS l,
  unit AS u,
  loan as lo
WHERE
  u.pk = lo.source_word_pk
  AND lo.target_word_pk in (
    SELECT cp.word_pk 
    from counterpart as cp, valueset as vs, value as v, contribution as c 
    where 
      c.id = '{0}' 
      and vs.contribution_pk = c.pk 
      and cp.pk = v.pk 
      and v.valueset_pk = vs.pk) 
  AND u.language_pk = l.pk
ORDER BY
  l.name"""

# The mapping between WOLD languages and Glottocodes or ISO 639-3 codes is many-to-many,
# this the query below may return more than one code per type/language pair.
SQL_IDENTIFIERS = """\
SELECT
  li.language_pk, i.type, i.name
FROM
  languageidentifier as li, identifier as i
WHERE
  (i.type = 'glottolog' OR i.type = 'iso639-3') AND li.identifier_pk = i.pk
ORDER BY
  li.language_pk, i.type"""

SQL_MEANING = """\
SELECT 
  p.id, p.name, m.semantic_category, sf.id, sf.name
FROM
  parameter AS p, 
  meaning AS m,
  semanticfield as sf 
WHERE
  p.pk = m.pk 
  AND m.semantic_field_pk = sf.pk
ORDER BY
  p.pk"""


def make_cldf(db, out, mid):
    # Initialize a CLDF dataset in the output directory, using the appropriate module:
    ds = Wordlist.in_dir(out)

    # Source words are not coded for meaning slots, so we have to relax the schema:
    ds['FormTable', 'Parameter_ID'].required = False

    # We add the WOLD language metadata:
    ds.add_component('LanguageTable')

    # some metadata about the comparison meanings:
    ds.add_component('ParameterTable', 'Category', 'SemanticField_ID', 'SemanticField')

    # and the information on borrowings (aka loanwords):
    ds.add_component(
        'BorrowingTable',
        {
            'name': 'relation',
            'datatype': {'base': 'string', 'format': 'immediate|earlier'}},
        {'name': 'certain', 'datatype': 'boolean'})

    # Now we collect the data by querying the database:
    forms, languages = [], {}

    lids = defaultdict(dict)
    for lpk, ids in groupby(db.execute(SQL_IDENTIFIERS), lambda r: r[0]):
        for itype, names in groupby(ids, lambda rr: rr[1]):
            names = [n[2] for n in names]
            if len(names) == 1:
                # only add identifiers for equivalent languoids, ignore partial matches.
                lids[lpk][itype] = names[0]

    pids = set()  # store all meaning IDs occurring for any form
    upk2uid = {}  # store the mapping of word pks to Form_ID, for relating loans
    for row in db.execute(SQL_FORMS.format(mid)):
        lpk, lid, lname, vspk, vid, pid, uname, upk = row
        upk2uid[upk] = vid
        ids = lids.get(lpk, {})
        pids.add(pid)
        languages[lpk] = dict(
            ID=lid,
            Name=lname,
            glottocode=ids.get('glottolog'),
            Iso=ids.get('iso639-3'),
        )
        forms.append(dict(
            ID=vid,
            Language_ID=lid,
            Parameter_ID=pid,
            Form=uname,
        ))

    borrowings = []
    sourceforms = {}
    for i, row in enumerate(db.execute(SQL_SOURCE_FORMS.format(mid))):
        lpk, lid, lname, form, uid, tpk, lrel, lcertain = row
        ids = lids.get(lpk, {})
        if form != 'Unidentifiable':
            borrowings.append(dict(
                ID='{0}'.format(i + 1),
                Form_ID_Source=uid,
                Form_ID_Target=upk2uid[tpk],
                relation=lrel,
                certain=lcertain,
            ))
            sourceforms[uid] = dict(
                ID=uid,
                Language_ID=lid,
                Parameter_ID=None,
                Form=form,
            )
            languages[lpk] = dict(
                ID=lid,
                Name=lname,
                glottocode=ids.get('glottolog'),
                Iso=ids.get('iso639-3'),
            )

    meanings = []
    for row in db.execute(SQL_MEANING):
        id, name, semantic_category, sfid, sfname = row
        if id in pids:
            meanings.append(dict(
                ID=id,
                Name=name,
                Category=semantic_category,
                SemanticField_ID=sfid,
                SemanticField=sfname,
            ))

    ds.write(
        FormTable=forms + list(sourceforms.values()),
        ParameterTable=meanings,
        LanguageTable=languages.values(),
        BorrowingTable=borrowings,
    )
    ds.validate()


if __name__ == '__main__':
    import sys

    db = create_engine(sys.argv[1])
    feature = sys.argv[2]
    out = Path('wold_{0}_cldf'.format(feature))
    if not out.exists():
        out.mkdir()
    make_cldf(db, out, feature)
