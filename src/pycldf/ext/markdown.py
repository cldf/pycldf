"""
This module provides tools to build a CLDF Markdown renderer.

For an example, see :class:`FilenameToComponent`.
"""
import re
import typing
import pathlib
import warnings

import attr
import frontmatter
import clldutils
from clldutils.markup import MarkdownLink

from .discovery import get_dataset
from pycldf.util import pkg_path, url_without_fragment
from pycldf.dataset import MD_SUFFIX
from pycldf import Dataset
from pycldf import orm

__all__ = ['CLDFMarkdownLink', 'CLDFMarkdownText', 'FilenameToComponent']

# The YAML frontmatter key to specify dataset mappings:
DATASETS_MAPPING = 'cldf-datasets'


class DatasetMapping:
    key_pattern = re.compile('[a-zA-Z0-9_]+')

    @staticmethod
    def to_dict(o):
        if isinstance(o, DatasetMapping):
            return o.m
        return {} if not o else ({None: o} if isinstance(o, (str, Dataset)) else o)

    def __init__(self, m1, m2=None, doc_path=None, download_dir=None):
        self.m = self.to_dict(m1)
        self.m.update(self.to_dict(m2))
        if not all(True if k is None else DatasetMapping.key_pattern.fullmatch(k) for k in self.m):
            raise ValueError('Invalid dataset prefix')
        for k in self.m:
            if not isinstance(self.m[k], Dataset):
                self.m[k] = get_dataset(self.m[k], download_dir, doc_path)

    def __getitem__(self, item):
        return self.m[item]


@attr.s
class CLDFMarkdownLink(MarkdownLink):
    """
    CLDF markdown links are specified using URLs of a particular format.
    """
    fragment_pattern = re.compile(r'cldf(-(?P<dsid>[a-zA-Z0-9_]+))?:')

    @property
    def url_without_fragment(self):
        return url_without_fragment(self.parsed_url)

    @staticmethod
    def format_url(path, objid, dsid=None):
        return '{}#cldf{}:{}'.format(path, '-' + dsid if dsid else '', objid)

    @classmethod
    def from_component(cls, comp, objid='__all__', label=None, dsid=None) -> 'CLDFMarkdownLink':
        return cls(
            label=label or '{}:{}'.format(comp, objid),
            url=cls.format_url(comp, objid, dsid=dsid))

    @property
    def is_cldf_link(self) -> bool:
        return bool(self.fragment_pattern.match(self.parsed_url.fragment))

    @property
    def dsid(self) -> typing.Union[None, str]:
        if self.is_cldf_link:
            return self.fragment_pattern.match(self.parsed_url.fragment).group('dsid')

    @property
    def table_or_fname(self) -> typing.Union[None, str]:
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
            return name if name == 'Source' or pkg_path('components', name + MD_SUFFIX).exists() \
                else None

        if isinstance(cldf, (dict, DatasetMapping)):
            cldf = cldf[self.dsid]

        if name == cldf.bibname or name == 'Source':
            return 'Source'
        try:
            return cldf.get_tabletype(cldf[name])
        except (KeyError, ValueError):
            return None

    @property
    def objid(self) -> typing.Union[None, str]:
        if self.is_cldf_link:
            return self.parsed_url.fragment.split(':', maxsplit=1)[-1]

    @property
    def all(self) -> bool:
        return self.objid == '__all__'

    def get_row(self, cldf) -> dict:
        assert self.is_cldf_link and self.objid and (not self.all)
        ds = DatasetMapping(cldf)[self.dsid]
        return ds.get_row(self.component(cldf=ds), self.objid)

    def get_object(self, cldf) -> orm.Object:
        assert self.is_cldf_link and self.objid and (not self.all)
        ds = DatasetMapping(cldf)[self.dsid]
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
    """
    def __init__(self,
                 text: typing.Union[pathlib.Path, str],
                 dataset_mapping: typing.Optional[typing.Dict[str, Dataset]] = None,
                 download_dir: typing.Optional[pathlib.Path] = None):
        """
        :param text: CLDF Markdown text either to be read from a path or specified as `str`.
        :param dataset_mapping: Mapping of dataset prefixes to `Dataset` instances. May override \
        the mapping provided in YAML frontmatter as part of the text.
        :download_dir: Optional path to a directory to download data for remote datasets.
        """
        p = frontmatter.loads(text) if isinstance(text, str) else frontmatter.load(str(text))
        self.dataset_mapping = DatasetMapping(
            p.get(DATASETS_MAPPING),
            dataset_mapping,
            text.parent if isinstance(text, pathlib.Path) else None,
            download_dir,
        )
        self.text = p.content

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
            if simple_link_detection or markdown_kw:
                warnings.warn(
                    'Extended markdown link detection is only supported with clldutils>=3.14',
                    category=UserWarning)
            kw = {}
        else:
            kw = dict(simple=simple_link_detection, markdown_kw=markdown_kw)
        return CLDFMarkdownLink.replace(self.text, self._render_link, **kw)


class FilenameToComponent(CLDFMarkdownText):
    """
    Renderer to replace filenames in CLDF Markdown links with Component names.
    """
    def render_link(self, cldf_link):
        """
        Rewrites to URL of CLDF Markdown links, using the component name for the part before the
        fragment.
        """
        return cldf_link.update_url(path=cldf_link.component(cldf=self.dataset_mapping))
