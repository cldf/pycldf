"""
Check the content of a valid CLDF dataset
- Is any of the tables declared by the dataset empty?

Column checks:
"""
import contextlib

from clldutils import iso_639_3

from pycldf.cli_util import add_catalog_spec, add_dataset, get_dataset, FlagOrPathType

try:
    from cldfcatalog import Catalog
    from pyglottolog import Glottolog
    from pyconcepticon import Concepticon
except ImportError:  # pragma: no cover
    Catalog, Glottolog, Concepticon = None, None, None


def register(parser):  # pylint: disable=C0116
    add_dataset(parser)
    add_catalog_spec(parser, 'glottolog')
    add_catalog_spec(parser, 'concepticon')
    parser.add_argument(
        '--iso-codes',
        help='Check ISO codes against an ISO 639-3 code table. Pass a file name pointing to '
             'a download of \nhttps://iso639-3.sil.org/sites/iso639-3/files/downloads/iso-639-3.tab'
             '\nor "y" to download the code table on the fly.',
        type=FlagOrPathType(type='file'),
        default='n',
    )


def run(args):  # pylint: disable=C0116
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
        if args.concepticon_version:  # pragma: no cover
            stack.enter_context(Catalog(args.concepticon, tag=args.concepticon_version))

        for table, checkers in COLUMN_CHECKERS.items():
            _check_table(ds, table, checkers, args, warn)

        for table in ds.tables:
            for _ in table:
                break
            else:
                warn(f'Empty table {table.url}')

    return 2 if warnings else 0


def _check_table(ds, table, checkers, args, warn):
    table = ds.get(table)
    if not table:
        return
    idcol = ds.get((table, 'id'))
    active_checkers = {}
    for col, checker in checkers.items():
        col = ds.get((table, col))
        if col:
            # Register an initialized check:
            active_checkers[col.name] = checker(args)
    if active_checkers:
        for row in table:
            rowid = row[idcol.name] if idcol else str(row)
            for colname, check in active_checkers.items():
                check(row[colname], rowid, warn)


class Check:  # pylint: disable=R0903
    """A base class for checks. Initialize with __init__ then run __call__ on each row."""
    def __init__(self, args):
        self.args = args

    def __call__(self, gc, rowid, warn):
        raise NotImplementedError()  # pragma: no cover


class Macroarea(Check):  # pylint: disable=R0903
    """Is the macroarea valid according to Glottolog? (requires "--glottolog")"""
    def __init__(self, args):
        super().__init__(args)
        if args.glottolog:
            self.macroareas = [ma.name for ma in Glottolog(args.glottolog).macroareas.values()]
        else:
            self.macroareas = None

    def __call__(self, ma, rowid, warn):
        if self.macroareas and ma and (ma not in self.macroareas):
            warn(f'Language {rowid} assigned to invalid macroarea {ma}')


class Glottocode(Check):  # pylint: disable=R0903
    """Is the Glottocode valid - is it in Bookkeeping? (requires "--glottolog")"""
    def __init__(self, args):
        super().__init__(args)
        self.bookkeeping, self.gcs = None, None
        if args.glottolog:
            glottolog = Glottolog(args.glottolog)
            self.bookkeeping, self.gcs = set(), set()
            for lang in glottolog.languoids():
                self.gcs.add(lang.id)
                if lang.lineage and lang.lineage[0][1] == \
                        glottolog.language_types['bookkeeping'].pseudo_family_id:
                    self.bookkeeping.add(lang.id)

    def __call__(self, gc, rowid, warn):
        if self.gcs and gc:
            if gc in self.bookkeeping:
                warn(f'Language {rowid} mapped to Bookkeeping languoid {gc}')
            if gc not in self.gcs:
                warn(f'Language {rowid} mapped to invalid Glottocode {gc}')


class ISOCode(Check):  # pylint: disable=R0903
    """Is the ISO code valid? (requires "--iso-codes")"""
    def __init__(self, args):
        super().__init__(args)
        if args.iso_codes is True:  # pragma: no cover
            # Check ISO codes against a code table from the ISO 639-3 website:
            iso_codes = iso_639_3._open(
                '/sites/iso639-3/files/downloads/iso-639-3.tab').read().decode('utf8')
        elif args.iso_codes:
            iso_codes = args.iso_codes.read_text(encoding='utf8')
        else:
            iso_codes = None
        if iso_codes:
            iso_codes = {r.split('\t')[0] for r in iso_codes.split('\n')}
        self.iso_codes = {r for r in iso_codes if len(r) == 3} if iso_codes else None

    def __call__(self, iso, rowid, warn):
        if self.iso_codes and iso and (iso not in self.iso_codes):
            warn(f'Language {rowid} mapped to invalid ISO 639-3 code {iso}')


class Latitude(Check):  # pylint: disable=R0903
    """Is the latitude between -90 and 90?"""
    def __call__(self, lat, rowid, warn):
        if lat and not -90 <= lat <= 90:
            warn(f'Language {rowid} has invalid latitude {lat}')


class Longitude(Check):  # pylint: disable=R0903
    """Is the longitude between -180 and 180?"""
    def __call__(self, lon, rowid, warn):
        if lon and not -180 <= lon <= 180:
            warn(f'Language {rowid} has invalid longitude {lon}')


class ConcepticonID(Check):  # pylint: disable=R0903
    """Is the concept set ID valid? (requires "--concepticon")"""
    def __init__(self, args):
        super().__init__(args)
        if args.concepticon:
            api = Concepticon(args.concepticon)
            self.ids = set(api.conceptsets)
        else:
            self.ids = None

    def __call__(self, cid, rowid, warn):
        if self.ids and cid and (cid not in self.ids):
            warn(f'Parameter {rowid} mapped to invalid conceptset ID {cid}')


COLUMN_CHECKERS = {
    'LanguageTable': {
        'glottocode': Glottocode,
        'iso639P3code': ISOCode,
        'latitude': Latitude,
        'longitude': Longitude,
        'macroarea': Macroarea,
    },
    'ParameterTable': {
        'concepticonReference': ConcepticonID,
    }
}
for t, checks in COLUMN_CHECKERS.items():
    __doc__ += f'\n- {t}\n'
    for c, cls in checks.items():
        __doc__ += f'  - {c}: {cls.__doc__.strip()}\n'
