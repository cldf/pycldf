"""
Check the content of a valid CLDF dataset
"""
import contextlib

from pycldf.cli_util import add_catalog_spec, add_dataset, get_dataset

try:
    from cldfcatalog import Catalog
    from pyglottolog import Glottolog
except ImportError:  # pragma: no cover
    Catalog, Glottolog = None, None


def register(parser):
    add_dataset(parser)
    add_catalog_spec(parser, 'glottolog')


def run(args):
    if not Catalog:  # pragma: no cover
        print('\nThis command only works with catalogs installed.\n'
              'Run "pip install pycldf[catalogs]" to do so.\n')
        return 1

    warnings = []

    def warn(msg):
        warnings.append(msg)
        args.log.warning(msg)

    ds = get_dataset(args)

    with contextlib.ExitStack() as stack:
        if args.glottolog_version:  # pragma: no cover
            stack.enter_context(Catalog(args.glottolog, tag=args.glottolog_version))

        ltable, gccol = None, None

        try:
            ltable = ds['LanguageTable']
            gccol = ds['LanguageTable', 'glottocode']
        except KeyError:
            pass

        if gccol:
            glottolog = Glottolog(args.glottolog)
            bookkeeping, gcs = set(), set()
            for l in glottolog.languoids():
                gcs.add(l.id)
                if l.lineage and l.lineage[0][1] == 'book1242':
                    bookkeeping.add(l.id)

        if ltable:
            nlanguages = 0
            for nlanguages, row in enumerate(ltable, start=1):
                if gccol:
                    # no bookkeeping languages
                    gc = row[gccol.name]
                    if gc:
                        if gc in bookkeeping:
                            warn('Language {0} mapped to Bookkeeping languoid {1}'.format(
                                row['ID'], gc))
                        if gc not in gcs:
                            warn('Language {0} mapped to invalid Glottocode {1}'.format(
                                row['ID'], gc))

            if not nlanguages:
                warn('No languages in dataset')

    return 2 if warnings else 0
