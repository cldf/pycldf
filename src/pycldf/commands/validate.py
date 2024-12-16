"""
Validate a dataset against the CLDF specification, i.e. check
- whether required tables and columns are present
- whether values for required columns are present
- the referential integrity of the dataset
"""
import collections

from pycldf.cli_util import add_dataset, get_dataset
from pycldf.media import MediaTable
from pycldf.ext.markdown import CLDFMarkdownText


def register(parser):
    add_dataset(parser)
    parser.add_argument(
        '--with-cldf-markdown',
        default=False,
        action='store_true',
        help='Make sure that links in CLDF Markdown content in table rows can be resolved.',
    )


def run(args):
    cldf = get_dataset(args)
    if not cldf.validate(log=args.log):
        return 1

    if not args.with_cldf_markdown:
        return 0

    missing = collections.Counter()
    links = []

    class TestMarkdown(CLDFMarkdownText):
        def render_link(self, cldf_link):
            links.append(cldf_link)
            try:
                self.get_object(cldf_link)
            except:  # noqa: E722
                missing.update(['{}:{}:{}'.format(
                    cldf_link.label, cldf_link.table_or_fname, cldf_link.objid)])

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
        args.log.info('Validating CLDF Markdown links in {}:{}'.format(t, c))
        for obj in cldf[t]:
            if obj[c] and '[' in obj[c]:
                TestMarkdown(obj[c], cldf).render()

        for k, v in missing.most_common():
            res = 1
            args.log.warning('Not found {} ({} occurrences)'.format(k, v))
        args.log.info('{} links checked'.format(len(links)))
        missing, links = collections.Counter(), []

    if 'MediaTable' in cldf and ('MediaTable', 'http://purl.org/dc/terms/conformsTo') in cldf:
        ctcol = cldf['MediaTable', 'http://purl.org/dc/terms/conformsTo']
        for file in MediaTable(cldf):
            if file.row[ctcol.name] == 'CLDF Markdown':
                args.log.info('Validating CLDF Markdown links in MediaTable:{}'.format(file.id))
                TestMarkdown(file.read(), cldf).render()
                for k, v in missing.most_common():
                    res = 1
                    args.log.warning('Not found {} ({} occurrences)'.format(k, v))
                args.log.info('{} links checked'.format(len(links)))
                missing, links = collections.Counter(), []

    return res
