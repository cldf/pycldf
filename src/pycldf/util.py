import shutil
from typing import Optional, TYPE_CHECKING, Any
import pathlib
import itertools
import collections
import urllib.parse
import urllib.request

from csvw.metadata import is_url, Link
from clldutils.path import git_describe

from pycldf.fileutil import PathType
from pycldf.urlutil import sanitize_url

if TYPE_CHECKING:
    from pycldf import Dataset  # pragma: no cover

__all__ = [
    'pkg_path', 'multislice', 'resolve_slices', 'DictTuple', 'qname2url',
    'iter_uritemplates', 'MD_SUFFIX', 'GitRepository']

MD_SUFFIX = '-metadata.json'


class GitRepository:  # pylint: disable=too-few-public-methods
    """
    CLDF datasets are often created from data curated in git repositories. If this is the case, we
    exploit this to provide better provenance information in the dataset's metadata.
    """
    def __init__(
            self, url: str, clone: Optional[PathType] = None, version: Optional[str] = None, **dc):
        # We remove credentials from the URL immediately to make sure this isn't leaked into
        # CLDF metadata. Such credentials might be present in URLs read via gitpython from
        # remotes.
        self.url = sanitize_url(url)
        self.clone = clone
        self.version = version
        self.dc = dc

    def json_ld(self) -> collections.OrderedDict[str, Any]:
        """The repository described in JSON-LD."""
        res = collections.OrderedDict([
            ('rdf:about', self.url),
            ('rdf:type', 'prov:Entity'),
        ])
        if self.version:
            res['dc:created'] = self.version
        elif self.clone:
            res['dc:created'] = git_describe(self.clone)
        res.update({f'dc:{k}': self.dc[k] for k in sorted(self.dc)})
        return res


def iter_uritemplates(table):
    props = ['aboutUrl', 'valueUrl']
    for obj in [table, table.tableSchema] + table.tableSchema.columns:
        for prop in props:
            tmpl = getattr(obj, prop)
            if tmpl:
                yield obj, prop, tmpl


def pkg_path(*comps):
    return pathlib.Path(__file__).resolve().parent.joinpath(*comps)


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

    def __init__(self, _, key=lambda i: i.id, multi=False):
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
        return super().__getitem__(item)


def qname2url(qname):
    for prefix, uri in {
        'csvw': 'http://www.w3.org/ns/csvw#',
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
        'xsd': 'http://www.w3.org/2001/XMLSchema#',
        'dc': 'http://purl.org/dc/terms/',
        'dcat': 'http://www.w3.org/ns/dcat#',
        'prov': 'http://www.w3.org/ns/prov#',
    }.items():
        if qname.startswith(prefix + ':'):
            return qname.replace(prefix + ':', uri)


def copy_dataset(ds: 'Dataset', dest: PathType, mdname: str = None) -> pathlib.Path:
    """
    Copy metadata, data and sources to files in `dest`.
    """
    from pycldf.media import MediaTable  # pylint: disable=import-outside-toplevel

    dest = pathlib.Path(dest)
    if not dest.exists():
        dest.mkdir(parents=True)

    from_url = is_url(ds.tablegroup.base)
    ds = ds.__class__.from_metadata(
        ds.tablegroup.base if from_url else ds.tablegroup._fname)  # pylint: disable=W0212

    _getter = urllib.request.urlretrieve if from_url else shutil.copy
    try:
        _getter(ds.bibpath, dest / ds.bibname)
        ds.properties['dc:source'] = ds.bibname
    except:  # pragma: no cover # noqa  pylint: disable=W0702
        # Sources are optional
        pass

    for table in ds.tables:
        fname = table.url.resolve(table.base)
        name = pathlib.Path(urllib.parse.urlparse(fname).path).name if from_url else fname.name
        _getter(fname, dest / name)
        table.url = Link(name)

        for fk in table.tableSchema.foreignKeys:
            fk.reference.resource = Link(pathlib.Path(fk.reference.resource.string).name)
    mdpath = dest.joinpath(
        mdname or  # noqa: W504
        (ds.tablegroup.base.split('/')[-1] if from_url
         else ds.tablegroup._fname.name))  # pylint: disable=W0212
    if 'MediaTable' in ds:
        for f in MediaTable(ds):
            if f.scheme == 'file':
                if f.local_path().exists():
                    target = dest / urllib.parse.unquote(f.relpath)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy(f.local_path(), target)
    if from_url:
        del ds.tablegroup.at_props['base']  # pragma: no cover
    ds.write_metadata(fname=mdpath)
    return mdpath
