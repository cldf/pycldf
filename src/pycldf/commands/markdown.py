"""
Convert the information in CLDF metadata to markdown suitable for inclusion in a README
"""
import html

from clldutils.misc import slug
from pycldf.cli_util import add_dataset, get_dataset


def register(parser):
    add_dataset(parser)
    parser.add_argument(
        '--rel-path',
        help='relative path to use for links to data files',
        default='./')


def run(args):
    ds = get_dataset(args)
    title = ds.properties.get('dc:title', ds.module)
    print('# {}\n'.format(title))
    if args.dataset.suffix == '.json':
        print('**CLDF Metadata**: [{0}]({1}{0})\n'.format(args.dataset.name, args.rel_path))
    if 'dc:source' in ds.properties:
        print('**Sources**: [{0}]({1}{0})\n'.format(ds.properties['dc:source'], args.rel_path))
    properties(ds.tablegroup)

    for table in ds.tables:
        fks = {
            fk.columnReference[0]: (fk.reference.columnReference[0], fk.reference.resource.string)
            for fk in table.tableSchema.foreignKeys if len(fk.columnReference) == 1}
        print('\n## <a name="table-{0}"></a>Table [{1}]({2}{1})\n'.format(
            slug(table.url.string), table.url, args.rel_path))
        properties(table)
        print('\n### Columns\n')
        print('Name | Datatype | Property | Description')
        print(' --- | --- | --- | ---')
        for col in table.tableSchema.columns:
            print(colrow(col, fks))


def qname2link(qname, html=False):
    prefixes = {
        'csvw': 'http://www.w3.org/ns/csvw#',
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
        'xsd': 'http://www.w3.org/2001/XMLSchema#',
        'dc': 'http://purl.org/dc/terms/',
        'dcat': 'http://www.w3.org/ns/dcat#',
        'prov': 'http://www.w3.org/ns/prov#',
    }
    if ':' in qname:
        prefix, lname = qname.split(':', maxsplit=1)
        if prefix in prefixes:
            if html:
                return '<a href="{}{}">{}</a>'.format(prefixes[prefix], lname, qname)
            return '[{}]({}{})'.format(qname, prefixes[prefix], lname)
    return qname


def htmlify(obj):
    """
    For inclusion in tables we must use HTML for lists.
    """
    if isinstance(obj, list):
        return '<ol>{}</ol>'.format(''.join('<li>{}</li>'.format(htmlify(item)) for item in obj))
    if isinstance(obj, dict):
        if obj.get('rdf:type') == 'prov:Entity' and 'rdf:about' in obj:
            label = obj.get('dc:title')
            if (not label) or label == 'Repository':
                label = obj['rdf:about']
            url = obj['rdf:about']
            if ('github.com' in url) and ('/tree/' not in url) and ('dc:created' in obj):
                tag = obj['dc:created']
                if '-g' in tag:
                    tag = tag.split('-g')[-1]
                url = '{}/tree/{}'.format(url, tag)
            return '<a href="{}">{} {}</a>'.format(url, label, obj.get('dc:created') or '')
        items = []
        for k, v in obj.items():
            items.append('<dt>{}</dt><dd>{}</dd>'.format(
                qname2link(k, html=True), html.escape(str(v))))
        return '<dl>{}</dl>'.format(''.join(items))
    return str(obj)


def properties(obj):
    if obj.common_props.get('dc:description'):
        print(obj.common_props['dc:description'] + '\n')
    print('property | value\n --- | ---')
    for k, v in obj.common_props.items():
        if not v:
            continue
        if k not in ('dc:description', 'dc:title', 'dc:source'):
            if k == 'dc:conformsTo':
                v = '[CLDF {}]({})'.format(v.split('#')[1], v)
            print('{} | {}'.format(qname2link(k), htmlify(v)))
    print('')


def lname(url):
    url = getattr(url, 'uri', url)
    if '#' in url:
        return url.split('#')[-1]
    if '/' in url:  # pragma: no cover
        return url.split('/')[-1]
    return url  # pragma: no cover


def colrow(col, fks):
    dt = '`{}`'.format(col.datatype.base if col.datatype else 'string')
    if col.separator:
        dt = 'list of {} (separated by `{}`)'.format(dt, col.separator)
    desc = col.common_props.get('dc:description', '').replace('\n', ' ')

    if col.name in fks:
        desc += '<br>References [{}::{}](#table-{})'.format(
            fks[col.name][1], fks[col.name][0], slug(fks[col.name][1]))

    return ' | '.join([
        '`{}`'.format(col.name),
        dt,
        '[{}]({})'.format(lname(col.propertyUrl), col.propertyUrl) if col.propertyUrl else '',
        desc,
    ])
