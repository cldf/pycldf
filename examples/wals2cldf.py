import pathlib

from pycldf import StructureDataset


def make_cldf(out, fid):
    # Read the metadata directly from GitHub:
    wals = StructureDataset.from_metadata(
        'https://raw.githubusercontent.com/cldf-datasets/wals/v2020/cldf/StructureDataset-metadata.json')
    for row in wals.iter_rows('ParameterTable', 'id', 'name'):
        if row['id'] == fid:
            feature = row
            break
    else:
        raise ValueError('Invalid feature id: {}'.format(fid))

    # Initialize a CLDF dataset in the output directory, using the appropriate module:
    ds = StructureDataset.in_dir(out)

    # We add the WALS language metadata:
    ds.add_component('LanguageTable', 'Genus', 'Family')

    # And some metadata about the feature:
    ds.add_component('ParameterTable', 'Authors', 'Url', 'Area')
    ds.add_component('CodeTable')

    # Now we collect the data by filtering the full dataset:
    values = []
    # We use `Dataset.objects` for easier access to the Source objects, referenced by the value.
    for val in wals.objects('ValueTable'):
        if val.cldf.parameterReference == fid:
            values.append(val)
            for ref in val.references:
                ds.add_sources(ref.source)

    # We use the `Dataset.iter_rows` to be able to access columns by CLDF ontology terms rather than
    # by dataset-local column names.
    languages = [
        r for r in wals.iter_rows('LanguageTable', 'id')
        if r['id'] in set(v.cldf.languageReference for v in values)]

    codes = [
        r for r in wals.iter_rows('CodeTable', 'id')
        if r['id'] in set(v.cldf.codeReference for v in values)]

    # Contributor names must be looked up in a non-CLDF table:
    authors = [r for r in wals['contributors.csv'] if r['ID'] in feature['Contributor_ID']]
    ds.write(
        ValueTable=[v.data for v in values],
        LanguageTable=languages,
        ParameterTable=[{
            'ID': fid,
            'Name': feature['name'],
            'Area': feature['Area'],
            'Authors': ' and '.join(a['Name'] for a in authors),
            'Url': 'https://wals.info/feature/' + fid}],
        CodeTable=codes,
    )


if __name__ == '__main__':
    import sys

    feature = sys.argv[1]
    out = pathlib.Path('wals_{0}_cldf'.format(feature))
    if not out.exists():
        out.mkdir()
    make_cldf(out, feature)
