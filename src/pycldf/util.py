import html
import pathlib
import itertools
import collections

from clldutils.misc import slug
import pycldf

__all__ = ['pkg_path', 'multislice', 'resolve_slices', 'DictTuple', 'metadata2markdown']


def pkg_path(*comps):
    return pathlib.Path(pycldf.__file__).resolve().parent.joinpath(*comps)


def multislice(sliceable, *slices):
    res = type(sliceable)()
    for sl in slices:
        if isinstance(sl, str):
            if ':' in sl:
                sl = [int(s) - (1 if i == 0 else 0) for i, s in enumerate(sl.split(':'))]
            else:
                sl = [int(sl) - 1, int(sl)]
        res += sliceable[slice(*sl)]
    return res


def resolve_slices(row, ds, slice_spec, target_spec, fk, target_row=None):
    # 1. Determine the slice column:
    slices = ds[slice_spec]

    # 2. Determine the to-be-sliced column:
    morphemes = ds[target_spec]

    # 3. Retrieve the matching row in the target table:
    target_row = target_row or ds.get_row(target_spec[0], row[fk])

    # 4. Slice the segments
    return list(itertools.chain(*[
        s.split() for s in multislice(target_row[morphemes.name], *row[slices.name])]))


class DictTuple(tuple):
    """
    A `tuple` that acts like a `dict` when a `str` is passed to `__getitem__`.

    Since CLDF requires a unique `id` for each row in a component, and recommends identifier of
    type `str`, this class can be used to provide convenient access to items in an ordered
    collection of such objects.
    """
    def __new__(cls, items, **kw):
        return super(DictTuple, cls).__new__(cls, tuple(items))

    def __init__(self, items, key=lambda i: i.id, multi=False):
        """
        If `key` does not return unique values for all items, you may pass `multi=True` to
        retrieve `list`s of matching items for `l[key]`.
        """
        self._d = collections.defaultdict(list)
        for i, o in enumerate(self):
            self._d[key(o)].append(i)
        self._multi = multi

    def __getitem__(self, item):
        if not isinstance(item, (int, slice)):
            if self._multi:
                return [self[i] for i in self._d[item]]
            return self[self._d[item][0]]
        return super(DictTuple, self).__getitem__(item)


def metadata2markdown(ds, path, rel_path='./'):
    """
    Render the metadata of a dataset as markdown.

    :param ds: `pycldf.Dataset` instance
    :param path: `pathlib.Path` of the metadata file
    :param rel_path: `str` to use a relative path when creating links to data files
    :return: `str` with markdown formatted text
    """
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

    def htmlify(obj, key=None):
        """
        For inclusion in tables we must use HTML for lists.
        """
        if isinstance(obj, list):
            return '<ol>{}</ol>'.format(
                ''.join('<li>{}</li>'.format(htmlify(item, key=key)) for item in obj))
        if isinstance(obj, dict):
            if key == 'prov:wasGeneratedBy' \
                    and set(obj.keys()).issubset({'dc:title', 'dc:description', 'dc:relation'}):
                desc = obj.get('dc:description') or ''
                if obj.get('dc:relation'):
                    desc = (desc + '<br>') if desc else desc
                    desc += '<a href="{0}{1}">{1}</a>'.format(rel_path, obj['dc:relation'])
                return '<strong>{}</strong>: {}'.format(obj.get('dc:title') or '', desc)

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
                    if label == obj['rdf:about']:
                        label = label.split('github.com/')[-1]
                return '<a href="{}">{} {}</a>'.format(url, label, obj.get('dc:created') or '')
            items = []
            for k, v in obj.items():
                items.append('<dt>{}</dt><dd>{}</dd>'.format(
                    qname2link(k, html=True), html.escape(str(v))))
            return '<dl>{}</dl>'.format(''.join(items))
        return str(obj)

    def properties(obj):
        res = []
        if obj.common_props.get('dc:description'):
            res.append(obj.common_props['dc:description'] + '\n')
        res.append('property | value\n --- | ---')
        for k, v in obj.common_props.items():
            if not v:
                continue
            if k not in ('dc:description', 'dc:title', 'dc:source'):
                if k == 'dc:conformsTo':
                    v = '[CLDF {}]({})'.format(v.split('#')[1], v)
                res.append('{} | {}'.format(qname2link(k), htmlify(v, key=k)))
        res.append('')
        return '\n'.join(res)

    def colrow(col, fks, pk):
        dt = '`{}`'.format(col.datatype.base if col.datatype else 'string')
        if col.separator:
            dt = 'list of {} (separated by `{}`)'.format(dt, col.separator)
        desc = col.common_props.get('dc:description', '').replace('\n', ' ')

        if col.name in pk:
            desc = (desc + '<br>') if desc else desc
            desc += 'Primary key'

        if col.name in fks:
            desc = (desc + '<br>') if desc else desc
            desc += 'References [{}::{}](#table-{})'.format(
                fks[col.name][1], fks[col.name][0], slug(fks[col.name][1]))
        elif col.propertyUrl \
                and col.propertyUrl.uri == "http://cldf.clld.org/v1.0/terms.rdf#source" \
                and 'dc:source' in ds.properties:
            desc = (desc + '<br>') if desc else desc
            desc += 'References [{}::BibTeX-key]({}{})'.format(
                ds.properties['dc:source'], rel_path, ds.properties['dc:source'])

        return ' | '.join([
            '[{}]({})'.format(col.name, col.propertyUrl)
            if col.propertyUrl else '`{}`'.format(col.name),
            dt,
            desc,
        ])

    title = ds.properties.get('dc:title', ds.module)

    res = ['# {}\n'.format(title)]
    if path.suffix == '.json':
        res.append('**CLDF Metadata**: [{0}]({1}{0})\n'.format(path.name, rel_path))
    if 'dc:source' in ds.properties:
        res.append('**Sources**: [{0}]({1}{0})\n'.format(ds.properties['dc:source'], rel_path))
    res.append(properties(ds.tablegroup))

    for table in ds.tables:
        fks = {
            fk.columnReference[0]: (fk.reference.columnReference[0], fk.reference.resource.string)
            for fk in table.tableSchema.foreignKeys if len(fk.columnReference) == 1}
        res.append('\n## <a name="table-{0}"></a>Table [{1}]({2}{1})\n'.format(
            slug(table.url.string), table.url, rel_path))
        res.append(properties(table))
        res.append('\n### Columns\n')
        res.append('Name/Property | Datatype | Description')
        res.append(' --- | --- | --- ')
        for col in table.tableSchema.columns:
            res.append(colrow(col, fks, table.tableSchema.primaryKey))
    return '\n'.join(res)
