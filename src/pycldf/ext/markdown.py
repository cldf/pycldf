"""
This module provides tools to build a CLDF Markdown renderer.

For an example, see :class:`FilenameToComponent`.
"""
import re
import typing
import pathlib
import warnings
import collections.abc

import yaml
import jmespath
import attr
import frontmatter
import clldutils
from clldutils.markup import MarkdownLink

from .discovery import get_dataset
from pycldf.util import pkg_path, url_without_fragment
from pycldf.dataset import MD_SUFFIX
from pycldf.sources import Source
from pycldf import Dataset
from pycldf import orm

__all__ = ['CLDFMarkdownLink', 'CLDFMarkdownText', 'FilenameToComponent']

#: The YAML frontmatter key to specify dataset mappings:
DATASETS_MAPPING = 'cldf-datasets'
SOURCE_COMPONENT = 'Source'
METADATA_COMPONENT = 'Metadata'


class DatasetMapping(collections.abc.Mapping):
    """
    A read-only mapping of prefixes to datasets.
    """
    key_pattern = re.compile('[a-zA-Z0-9_]+')

    @staticmethod
    def to_dict(o):
        if isinstance(o, DatasetMapping):
            return o.m
        return {} if not o else ({None: o} if isinstance(o, (str, Dataset)) else o)

    def __init__(self,
                 m1,
                 m2=None,
                 doc_path: typing.Optional[pathlib.Path] = None,
                 download_dir: typing.Optional[pathlib.Path] = None):
        """
        :param m1: Mapping of prefixes to datasets (locators).
        :param m2: Mapping of prefixes to datasets (locators) to update `m1`.
        :param doc_path: Path of a CLDF markdown document, relative to which dataset locators are \
        to be resolved.
        :param download_dir: Path to an existing directory to which to download datasets \
        (if necessary).
        """
        self.m = self.to_dict(m1)
        self.m.update(self.to_dict(m2))
        if not all(True if k is None else DatasetMapping.key_pattern.fullmatch(k) for k in self.m):
            raise ValueError('Invalid dataset prefix')
        for k in self.m:
            if not isinstance(self.m[k], Dataset):
                self.m[k] = get_dataset(self.m[k], download_dir, doc_path)

    def __getitem__(self, prefix: typing.Union[str, None]) -> Dataset:
        """
        Get a `Dataset` mapped to a prefix.
        """
        return self.m[prefix]

    def __iter__(self):
        return iter(self.m)

    def __len__(self):
        return len(self.m)


@attr.s
class CLDFMarkdownLink(MarkdownLink):
    """
    CLDF Markdown links are specified using URLs of a particular format.

    Instances of `CLDFMarkdownLink` are supplied as sole argument when calling the replacement
    function passed to `CLDFMarkdownLink.replace` .
    """
    fragment_pattern = re.compile(r'cldf(-(?P<prefix>[a-zA-Z0-9_]+))?:')

    @property
    def url_without_fragment(self):
        return url_without_fragment(self.parsed_url)

    @staticmethod
    def format_url(path, objid, prefix=None):
        return '{}#cldf{}:{}'.format(path, '-' + prefix if prefix else '', objid)

    @classmethod
    def from_component(cls, comp, objid='__all__', label=None, prefix=None) -> 'CLDFMarkdownLink':
        return cls(
            label=label or '{}:{}'.format(comp, objid),
            url=cls.format_url(comp, objid, prefix=prefix))

    @property
    def is_cldf_link(self) -> bool:
        """
        Flag signaling whether the markdown link is indeed referencing a CLDF object.
        """
        return bool(self.fragment_pattern.match(self.parsed_url.fragment))

    @property
    def prefix(self) -> typing.Union[None, str]:
        """
        The dataset prefix associated with a CLDF Markdown link.
        """
        if self.is_cldf_link:
            return self.fragment_pattern.match(self.parsed_url.fragment).group('prefix')

    @property
    def table_or_fname(self) -> typing.Union[None, str]:
        """
        The last path component of the URL of a CLDF Markdown link.
        """
        if self.is_cldf_link:
            return self.parsed_url.path.split('/')[-1]

    def component(self,
                  cldf: typing.Optional[
                      typing.Union[Dataset, typing.Dict[str, Dataset], DatasetMapping]] = None,
                  ) -> typing.Union[str, None]:
        """
        :param cldf: `pycldf.Dataset` instance to which the link refers.
        :return: Name of the CLDF component the link pertains to or `None`.
        """
        name = self.table_or_fname
        if cldf is None:
            # If no CLDF dataset is passed as context, we can only detect links using proper
            # component names as path:
            return name if (name in [SOURCE_COMPONENT, METADATA_COMPONENT] or  # noqa: W504
                            pkg_path('components', name + MD_SUFFIX).exists()) \
                else None

        if isinstance(cldf, (dict, DatasetMapping)):
            cldf = cldf[self.prefix]

        if name == cldf.bibname or name == SOURCE_COMPONENT:
            return SOURCE_COMPONENT
        if name == cldf.filename or name == METADATA_COMPONENT:
            return METADATA_COMPONENT
        try:
            return cldf.get_tabletype(cldf[name])
        except (KeyError, ValueError):
            return None

    @property
    def objid(self) -> typing.Union[None, str]:
        """
        The identifier of the object referenced by a CLDF Markdown link.
        """
        if self.is_cldf_link:
            return self.parsed_url.fragment.split(':', maxsplit=1)[-1]

    @property
    def all(self) -> bool:
        """
        Flag signaling whether the link is referencing the special `__all__` identifier.
        """
        return self.objid == '__all__'

    def get_row(self, cldf: typing.Union[Dataset, DatasetMapping]) -> dict:
        """
        Resolve the reference in a CLDF Markdown link to a row (`dict`) in the CLDF `Dataset`.
        """
        assert self.is_cldf_link and self.objid and (not self.all)
        ds = DatasetMapping(cldf)[self.prefix]
        return ds.get_row(self.component(cldf=ds), self.objid)

    def get_object(self, cldf: typing.Union[Dataset, DatasetMapping]) -> orm.Object:
        """
        Resolve the reference in a CLDF Markdown link to an ORM object in the CLDF `Dataset`.
        """
        assert self.is_cldf_link and self.objid and (not self.all)
        ds = DatasetMapping(cldf)[self.prefix]
        return ds.get_object(self.component(cldf=ds), self.objid)


class CLDFMarkdownText:
    """
    A CLDF Markdown document.

    Basic CLDF Markdown rendering can be implemented by overwriting the `render_link` method.
    Then, calling the `render` method will return a markdown string with CLDF Markdown links
    replaced.

    A trivial renderer, replacing each CLDF Markdown link with the link label, would look as
    follows:

    .. code-block:: python

        from pycldf.ext.markdown import CLDFMarkdownText

        class Renderer(CLDFMarkdownText):
            def render_link(self, link):
                return str(link.label)

        assert Renderer('[Example 1](ExampleTable#cldf:ex1)').render() == 'Example 1'

    :ivar text: `str` containing the markdown text (with YAML frontmatter removed).
    :ivar metadata: `dict` of document metadata read from YAML frontmatter.
    :ivar dataset_mapping: :class:`DatasetMapping` instance, linking prefixes used in CLDF \
    Markdown links to :class:`pycldf.Dataset` instances.
    :cvar source_component: Name of the special "Source" component.
    :cvar metadata_component: Name of the special "Metadata" component.
    """
    def __init__(self,
                 text: typing.Union[pathlib.Path, str],
                 dataset_mapping: typing.Optional[typing.Union[str, Dataset, dict]] = None,
                 download_dir: typing.Optional[pathlib.Path] = None):
        """
        :param text: CLDF Markdown text either to be read from a path or specified as `str`.
        :param dataset_mapping: Mapping of dataset prefixes to `Dataset` instances. May override \
        the mapping provided in YAML frontmatter as part of the text.
        :download_dir: Optional path to a directory to download data for remote datasets.
        """
        p = frontmatter.loads(text) if isinstance(text, str) else frontmatter.load(str(text))
        self.metadata = p.metadata
        self.dataset_mapping = DatasetMapping(
            p.get(DATASETS_MAPPING),
            dataset_mapping,
            text.parent if isinstance(text, pathlib.Path) else None,
            download_dir,
        )
        self.text = p.content
        self._datadict = collections.defaultdict(dict)
        for prefix, ds in self.dataset_mapping.items():
            self._datadict[prefix][SOURCE_COMPONENT] = {src.id: src for src in ds.sources}
            self._datadict[prefix][METADATA_COMPONENT] = ds.tablegroup.asdict(omit_defaults=True)

    @property
    def frontmatter(self) -> str:
        """
        The markdown documents metadata formatted as YAML frontmatter.
        """
        return '---\n{}---'.format(yaml.dump(self.metadata))

    def get_object(self, ml: CLDFMarkdownLink) -> typing.Union[list, orm.Object, Source, dict]:
        """
        Resolve the reference in a CLDF Markdown link to the matching object from a mapped dataset.

        The returned object is

        - an :class:`pycldf.orm.Object` instance for items in ORM mapped components,
        - a row `dict` for items in custom tables,
        - a :class:`pycldf.sources.Source` instance for source references,
        - a `list` of the above for the special `__all__` identifier.
        - a `jmespath.search` result for referenced items in the Metadata component,

        This method can be used within :meth:`render_link` implementations.
        """
        cldf = self.dataset_mapping[ml.prefix]
        comp = ml.component(cldf)
        key = comp or ml.table_or_fname

        if key == METADATA_COMPONENT:
            if ml.all:
                return self._datadict[ml.prefix][METADATA_COMPONENT]
            return jmespath.search(ml.objid, self._datadict[ml.prefix][METADATA_COMPONENT])

        if key not in self._datadict[ml.prefix]:  # A new type of data is referenced.
            objs = cldf.objects(comp) if comp else cldf.iter_rows(key, 'id')
            self._datadict[ml.prefix][key] = {
                r.id if isinstance(r, orm.Object) else r['id']: r for r in objs}
        return list(self._datadict[ml.prefix][key].values()) if ml.all \
            else self._datadict[ml.prefix][key][ml.objid]

    def _render_link(self, link):
        if link.is_cldf_link:
            return self.render_link(link)
        return link

    def render_link(self, cldf_link: CLDFMarkdownLink) -> typing.Union[str, CLDFMarkdownLink]:
        """
        CLDF Markdown renderers must implement this method.
        """
        raise NotImplementedError()  # pragma: no cover

    def render(self,
               simple_link_detection: bool = True,
               markdown_kw: typing.Optional[dict] = None) -> str:
        """
        A markdown string with CLDF Markdown links replaced.
        """
        if tuple(map(int, clldutils.__version__.split('.')[:2])) < (3, 14):  # pragma: no cover
            if not simple_link_detection or markdown_kw:
                warnings.warn(
                    'Extended markdown link detection is only supported with clldutils>=3.14',
                    category=UserWarning)
            kw = {}
        else:
            kw = dict(simple=simple_link_detection, markdown_kw=markdown_kw)
        return CLDFMarkdownLink.replace(self.text, self._render_link, **kw)


class FilenameToComponent(CLDFMarkdownText):
    """
    Renderer to replace filenames in CLDF Markdown links with CLDF component names.
    """
    def render_link(self, cldf_link):
        """
        Rewrites to URL of CLDF Markdown links, using the component name as path component.
        """
        comp = cldf_link.component(cldf=self.dataset_mapping)
        if comp:
            return cldf_link.update_url(path=cldf_link.component(cldf=self.dataset_mapping))
        return cldf_link
