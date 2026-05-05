"""
Validate a dataset against the CLDF specification, i.e. check
- whether required tables and columns are present
- whether values for required columns are present
- the referential integrity of the dataset
"""
import collections
import dataclasses

from pycldf import Dataset
from pycldf.cli_util import add_dataset, get_dataset
from pycldf.media import MediaTable
from pycldf.ext.markdown import CLDFMarkdownText, CLDFMarkdownLink


def register(parser):  # pylint: disable=C0116
    add_dataset(parser)
    parser.add_argument(
        '--with-cldf-markdown',
        default=False,
        action='store_true',
        help='Make sure that links in CLDF Markdown content in table rows can be resolved.',
    )


@dataclasses.dataclass
class TestMarkdown:
    """Helper class to run rendering of CLDF markdown and record results."""
    links: list[CLDFMarkdownLink] = dataclasses.field(default_factory=list)
    missing: collections.Counter = dataclasses.field(default_factory=collections.Counter)

    def __call__(self, text: str, ds: Dataset):
        class Parser(CLDFMarkdownText):
            """A CLDFMarkdownText subclass that records link render results."""
            def render_link(slf, cldf_link):  # pylint: disable=W0237,E0213
                self.links.append(cldf_link)
                try:
                    slf.get_object(cldf_link)
                except:  # noqa: E722  # pylint: disable=W0702
                    self.missing.update([
                        f'{cldf_link.label}:{cldf_link.table_or_fname}:{cldf_link.objid}'])
        Parser(text, ds).render()


def run(args):  # pylint: disable=C0116
    cldf = get_dataset(args)
    if not cldf.validate(log=args.log):
        return 1

    if not args.with_cldf_markdown:
        return 0

    cols = []
    for t in cldf.tables:
        try:
            tname = cldf.get_tabletype(t)
        except ValueError:
            tname = None
        tname = tname or str(t.url)
        for col in t.tableSchema.columns:
            if col.common_props.get('dc:conformsTo') == 'CLDF Markdown':
                cols.append((tname, col.name))

    res = 0
    for t, c in cols:
        tmd = TestMarkdown()
        args.log.info('Validating CLDF Markdown links in %s:%s', t, c)
        for obj in cldf[t]:
            if obj[c] and '[' in obj[c]:
                tmd(obj[c], cldf)

        for k, v in tmd.missing.most_common():
            res = 1
            args.log.warning('Not found %s (%s occurrences)', k, v)
        args.log.info('%s links checked', len(tmd.links))

    if 'MediaTable' in cldf and ('MediaTable', 'http://purl.org/dc/terms/conformsTo') in cldf:
        if not _validate_media(cldf, args.log):
            res = 1

    return res


def _validate_media(cldf, log) -> bool:
    res = True
    ctcol = cldf['MediaTable', 'http://purl.org/dc/terms/conformsTo']
    for file in MediaTable(cldf):
        if file.row[ctcol.name] == 'CLDF Markdown':
            log.info('Validating CLDF Markdown links in MediaTable:%s', file.id)
            tmd = TestMarkdown()
            tmd(file.read(), cldf)
            for k, v in tmd.missing.most_common():
                res = False
                log.warning('Not found %s (%s occurrences)', k, v)
            log.info('%s links checked', len(tmd.links))
    return res
