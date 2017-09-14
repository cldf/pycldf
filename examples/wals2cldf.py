from __future__ import unicode_literals
from collections import defaultdict
from itertools import groupby

from sqlalchemy import create_engine

from clldutils.path import Path
from pycldf.dataset import StructureDataset
from pycldf.sources import Source

# WALS only has one value per feature/language pair. So the query below, when parametrized
# with a feature ID, will return one row per distinct language.
SQL_VALUES = """\
SELECT 
  l.pk, 
  l.id, 
  l.name, 
  vs.id, 
  de.number,
  de.name,
  l.latitude, 
  l.longitude, 
  vs.pk,
  g.name,
  f.name 
FROM 
  value AS v, 
  valueset AS vs, 
  language AS l,
  walslanguage AS w,
  parameter AS p, 
  domainelement AS de,
  genus AS g,
  family AS f
WHERE 
  v.valueset_pk = vs.pk 
  AND vs.language_pk = l.pk 
  AND vs.parameter_pk = p.pk 
  AND p.id = '{0}' 
  AND v.domainelement_pk = de.pk
  AND l.pk = w.pk
  AND w.genus_pk = g.pk
  AND g.family_pk = f.pk"""

# The mapping between WALS languages and Glottocodes or ISO 639-3 codes is many-to-many,
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

SQL_SOURCES = """\
SELECT
  vsr.valueset_pk, vsr.description, s.id, s.bibtex_type, s.author, s.year, s.title
FROM
  valuesetreference AS vsr, source AS s 
WHERE
  vsr.source_pk = s.pk
ORDER BY
  vsr.valueset_pk"""

SQL_FEATURE = """\
SELECT 
  p.name, string_agg(c.name, ' and '), a.name
FROM
  parameter AS p, 
  feature AS f, 
  contributioncontributor AS cc, 
  contributor AS c,
  chapter AS ch,
  area AS a
WHERE
  p.id = '{0}'
  AND p.pk = f.pk 
  AND f.contribution_pk = cc.contribution_pk 
  AND cc.contributor_pk = c.pk
  AND f.contribution_pk = ch.pk
  AND ch.area_pk = a.pk
GROUP BY
  p.name, a.name"""


def make_cldf(db, out, fid):
    # Initialize a CLDF dataset in the output directory, using the appropriate module:
    ds = StructureDataset.in_dir(out)

    # We add the WALS language metadata:
    ds.add_component('LanguageTable', 'glottocode', 'iso639P3code', 'Genus', 'Family')

    # And some metadata about the feature:
    ds.add_component('ParameterTable', 'Authors', 'Url', 'Area')

    # Now we collect the data by querying the database:
    values, languages = [], []

    lids = defaultdict(dict)
    for lpk, ids in groupby(db.execute(SQL_IDENTIFIERS), lambda r: r[0]):
        for itype, names in groupby(ids, lambda rr: rr[1]):
            names = [n[2] for n in names]
            if len(names) == 1:
                # only add identifiers for equivalent languoids, ignore partial matches.
                lids[lpk][itype] = names[0]

    # We store the sources and references per datapoint:
    sources, refs = defaultdict(list), defaultdict(list)
    for vspk, rs in groupby(db.execute(SQL_SOURCES), lambda r: r[0]):
        for r in rs:
            ref = r[2]
            if r[1]:
                ref += '[{0}]'.format(r[1])  # add the page info in the correct format.
            refs[vspk].append(ref)
            sources[vspk].append(Source(r[3], r[2], author=r[4], year=r[5], title=r[6]))

    for row in db.execute(SQL_VALUES.format(fid)):
        lpk, lid, lname, vsid, denumber, dename, lat, lon, vspk, gname, fname = row
        ids = lids[lpk]
        if vspk in sources:
            ds.sources.add(*sources[vspk])
        languages.append(dict(
            ID=lid,
            Name=lname,
            Latitude=lat,
            Longitude=lon,
            glottocode=ids.get('glottolog'),
            iso639P3code=ids.get('iso639-3'),
            Genus=gname,
            Family=fname,
        ))
        values.append(dict(
            ID=vsid,
            Language_ID=lid,
            Parameter_ID=fid,
            Value=denumber,
            Source=refs.get(vspk, []),
            Comment=dename,
        ))

    fname, fauthors, aname = list(db.execute(SQL_FEATURE.format(fid)))[0]
    ds.write(
        ValueTable=values, 
        LanguageTable=languages, 
        ParameterTable=[{
            'ID': fid,
            'Name': fname,
            'Area': aname,
            'Authors': fauthors,
            'Url': 'http://wals.info/feature/' + fid}])


if __name__ == '__main__':
    import sys

    db = create_engine(sys.argv[1])
    feature = sys.argv[2]
    out = Path('wals_{0}_cldf'.format(feature))
    if not out.exists():
        out.mkdir()
    make_cldf(db, out, feature)
