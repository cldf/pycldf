"""
Functionality to render a Dataset's metadata to a Markdown document.
"""
import re
import html
import pathlib
from typing import TYPE_CHECKING, Any, Optional

from clldutils.misc import slug

from pycldf.util import qname2url
from pycldf.fileutil import PathType

if TYPE_CHECKING:
    from pycldf import Dataset  # pragma: no cover

__all__ = ['metadata2markdown']


def metadata2markdown(ds: 'Dataset', path: PathType, rel_path: Optional[str] = './') -> str:
    """
    Render the metadata of a dataset as markdown.

    :param ds: `Dataset` instance
    :param path: `pathlib.Path` of the metadata file
    :param rel_path: `str` to use a relative path when creating links to data files
    :return: `str` with markdown formatted text
    """
    path = pathlib.Path(path)
    return '\n'.join(_iter_markdown(ds, pathlib.Path(path), rel_path))


def _qname2link(qname: str, html_=False) -> str:
    url = qname2url(qname)
    if url:
        return f'<a href="{url}">{qname}</a>' if html_ else f'[{qname}]({url})'
    return qname


def _htmlify(obj: Any, rel_path: str, key=None) -> str:
    """
    For inclusion in tables we must use HTML for lists.
    """
    if isinstance(obj, list):
        items = [f'<li>{_htmlify(item, rel_path, key=key)}</li>' for item in obj]
        return f'<ol>{"".join(items)}</ol>'

    if isinstance(obj, dict):
        if key == 'prov:wasGeneratedBy' \
                and set(obj.keys()).issubset({'dc:title', 'dc:description', 'dc:relation'}):
            desc = obj.get('dc:description') or ''
            rel = obj.get('dc:relation')
            if rel:
                desc = (desc + '<br>') if desc else desc
                desc += f'<a href="{rel_path}{rel}">{rel}</a>'
            return f"<strong>{obj.get('dc:title') or ''}</strong>: {desc}"

        if obj.get('rdf:type') == 'prov:Entity' and 'rdf:about' in obj:
            label = obj.get('dc:title')
            if (not label) or label == 'Repository':
                label = obj['rdf:about']
            url = obj['rdf:about']
            if ('github.com' in url) and ('/tree/' not in url) and ('dc:created' in obj):
                tag = obj['dc:created']
                if '-g' in tag:
                    tag = tag.split('-g')[-1]
                url = f'{url}/tree/{tag}'
                if label == obj['rdf:about']:
                    label = label.split('github.com/')[-1]
            version = f' {obj.get("dc:created")}' or ''
            return f'<a href="{url}">{label} {version}</a>'

        items = [
            f'<dt>{_qname2link(k, html_=True)}</dt><dd>{html.escape(str(v))}</dd>'
            for k, v in obj.items()]
        return f'<dl>{"".join(items)}</dl>'

    return str(obj)


def _iter_properties(obj, rel_path):
    if obj.common_props.get('dc:description'):
        yield obj.common_props['dc:description'] + '\n'
    yield 'property | value\n --- | ---'
    for k, v in obj.common_props.items():
        if not v:
            continue
        if k not in ('dc:description', 'dc:title', 'dc:source'):
            if k == 'dc:conformsTo':
                v = f'[CLDF {v.split("#")[1]}]({v})'
            yield f'{_qname2link(k)} | {_htmlify(v, rel_path, key=k)}'
    yield ''


def _colrow(col, fks, pk, ds, rel_path):
    dt = f"`{col.datatype.base if col.datatype else 'string'}`"
    if col.datatype:
        if col.datatype.format:
            if re.fullmatch(r'[\w\s]+(\|[\w\s]+)*', col.datatype.format):
                dt += '<br>Valid choices:<br>'
                dt += ''.join(f' `{w}`' for w in col.datatype.format.split('|'))
            elif col.datatype.base == 'string':
                dt += f'<br>Regex: `{col.datatype.format}`'
        if col.datatype.minimum:
            dt += f'<br>&ge; {col.datatype.minimum}'
        if col.datatype.maximum:
            dt += f'<br>&le; {col.datatype.maximum}'
    if col.separator:
        dt = f'list of {dt} (separated by `{col.separator}`)'
    desc = col.common_props.get('dc:description', '').replace('\n', ' ')

    if col.name in pk:
        desc = (desc + '<br>') if desc else desc
        desc += 'Primary key'

    if col.name in fks:
        desc = (desc + '<br>') if desc else desc
        pkcol, table = fks[col.name]
        desc += f'References [{table}::{pkcol}](#table-{slug(table)})'
    elif col.propertyUrl \
            and col.propertyUrl.uri == "http://cldf.clld.org/v1.0/terms.rdf#source" \
            and 'dc:source' in ds.properties:
        desc = (desc + '<br>') if desc else desc
        desc += (f"References [{ds.properties['dc:source']}::BibTeX-key]"
                 f"({rel_path}{ds.properties['dc:source']})")

    return ' | '.join([
        f'[{col.name}]({col.propertyUrl})' if col.propertyUrl else f'`{col.name}`', dt, desc])


def _existing_fname_in_cldf_dir(ds, fname: str) -> Optional[str]:
    """Returns an existing (possibly zipped) file matching fname."""
    if pathlib.Path(ds.directory).joinpath(fname).exists():
        return fname
    zipped = fname + '.zip'
    if pathlib.Path(ds.directory).joinpath(zipped).exists():
        return zipped
    return None


def _iter_markdown(ds: 'Dataset', path: pathlib.Path, rel_path: Optional[str] = './'):
    def file_link(fname):
        return f'[{fname}]({rel_path}{fname})'

    yield f'# {ds.properties.get("dc:title", ds.module)}\n'
    if path.suffix == '.json':
        yield f'**CLDF Metadata**: {file_link(path.name)}\n'
    if 'dc:source' in ds.properties:
        src = _existing_fname_in_cldf_dir(ds, ds.properties['dc:source'])
        if src:
            yield f'**Sources**: {file_link(src)}\n'
    yield from _iter_properties(ds.tablegroup, rel_path)

    for table in ds.tables:
        fks = {
            fk.columnReference[0]: (fk.reference.columnReference[0], fk.reference.resource.string)
            for fk in table.tableSchema.foreignKeys if len(fk.columnReference) == 1}
        src = _existing_fname_in_cldf_dir(ds, table.url.string)
        table_name = file_link(src) if src else table.url
        yield f'\n## <a name="table-{slug(table.url.string)}"></a>Table {table_name}\n'
        yield from _iter_properties(table, rel_path)
        yield '\n### Columns\n'
        yield 'Name/Property | Datatype | Description'
        yield ' --- | --- | --- '
        for col in table.tableSchema.columns:
            yield _colrow(col, fks, table.tableSchema.primaryKey, ds, rel_path)
