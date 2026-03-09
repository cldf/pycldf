"""
This module provides a function (:func:`get_dataset`) implementing dataset discovery.

The scope of discoverable datasets can be extended by plugins, i.e. Python packages which
register additional :class:`DatasetResolver` subclasses using the entry point
`pycldf_dataset_resolver`

`pycldf` itself comes with two resolvers

- :class:`LocalResolver`
- :class:`GenericUrlResolver`

Additional resolvers:

- The `cldfzenodo <https://pypi.org/project/cldfzenodo>`_ package (>=1.0) provides a dataset
  resolver for DOI URLs pointing to the Zenodo archive.
"""
import re
from typing import Optional, Union
import pathlib
import zipfile
import warnings
import functools
import urllib.parse
import urllib.request
from importlib.metadata import entry_points

from csvw.utils import is_url

from pycldf import Dataset, iter_datasets, sniff
from pycldf.urlutil import url_without_fragment
from pycldf._compat import entry_points_select

__all__ = ['get_dataset', 'DatasetResolver']
EP = 'pycldf_dataset_resolver'

_resolvers = []


class DatasetResolver:  # pylint: disable=R0903
    """
    Virtual base class for dataset resolvers.

    :ivar priority: A number between 0 and 10, determining the call order of registered resolvers.\
    Resolvers with higher priority will be called earlier. Thus, resolvers specifying a high \
    priority should be quick in figuring out whether they apply to a locator.
    """
    priority = 5

    def __call__(
            self,
            loc: str,
            download_dir: pathlib.Path,
    ) -> Union[None, Dataset, pathlib.Path]:
        """
        :param loc: URL pointing to a place where datasets are archived.
        :param download_dir: A directory to which resolvers can download data.
        :return: Dataset resolvers may return `None` if they do not apply to `loc`, a `Dataset` \
        instance, if a candidate dataset was found, or a local path, pointing to a metadata file
        or a directory to be searched for metadata files.
        """
        raise NotImplementedError()  # pragma: no cover


class LocalResolver(DatasetResolver):  # pylint: disable=R0903
    """
    Resolves dataset locators specifying local file paths.
    """
    priority = 100

    def __call__(
            self,
            loc: str,
            download_dir,
            base: Optional[pathlib.Path],
    ) -> Optional[pathlib.Path]:
        """
        :return: a local path to a directory
        """
        if isinstance(loc, str) and is_url(loc):
            return None
        loc = pathlib.Path(loc)
        if loc.resolve() != loc and base:
            # A relative path, to be interpreted relative to base
            loc = base.resolve().joinpath(loc)
        if loc.exists():
            return loc
        return None  # pragma: no cover


class GenericUrlResolver(DatasetResolver):  # pylint: disable=R0903
    """
    URL resolver which works for generic URLs provided they point to a CLDF metadata file.
    """
    priority = -1

    def __call__(self, loc, download_dir) -> Optional[Dataset]:
        if is_url(loc):
            return Dataset.from_metadata(loc)
        return None  # pragma: no cover


class GitHubResolver(DatasetResolver):  # pylint: disable=R0903
    """
    Resolves dataset locators of the form "https://github.com/<org>/<repos>/tree/<tag>", e.g.
    https://github.com/cldf-datasets/petersonsouthasia/tree/v1.1
    or
    https://github.com/cldf-datasets/petersonsouthasia/releases/tag/v1.1
    """
    priority = 3

    def __call__(self, loc, download_dir) -> Optional[pathlib.Path]:
        url = urllib.parse.urlparse(loc)
        if url.netloc == 'github.com' and re.search(r'/[v.0-9]+$', url.path):
            comps = url.path.split('/')
            z = download_dir / f'{comps[1]}-{comps[2]}-{comps[-1]}.zip'
            url = f"https://github.com/{comps[1]}/{comps[2]}/archive/refs/tags/{comps[-1]}.zip"
            urllib.request.urlretrieve(url, z)
            with zipfile.ZipFile(z) as zf:
                dirs = {info.filename.split('/')[0] for info in zf.infolist()}
                assert len(dirs) == 1
                zf.extractall(download_dir)
            z.unlink()
            return download_dir / dirs.pop()
        return None


class DatasetLocator(str):
    """Dataset locators are URLs with identifying information added to the fragment."""
    @functools.cached_property
    def parsed_url(self) -> urllib.parse.ParseResult:  # pylint: disable=C0116
        return urllib.parse.urlparse(self)

    @property
    def url_without_fragment(self):  # pylint: disable=C0116
        return url_without_fragment(self.parsed_url)

    def match(self, dataset: Dataset) -> bool:  # pylint: disable=C0116
        if self.parsed_url.fragment:
            key, _, value = self.parsed_url.fragment.partition('=')
            return dataset.properties.get(key) == value if value else key in dataset.properties
        return True


def get_resolvers() -> list[type]:
    """Register resolvers defined via entry points."""
    if not _resolvers:
        for ep in set(entry_points_select(entry_points(), EP)):
            try:
                _resolvers.append(ep.load()())
            except ImportError:  # pragma: no cover
                warnings.warn(f'ImportError loading entry point {ep.name}')
                continue
    return sorted(_resolvers, key=lambda res: -res.priority)


def _get_dataset(
        locator: DatasetLocator,
        location: Union[None, Dataset, pathlib.Path],
) -> Optional[Dataset]:
    """Determine whether locator matches location and if so, resolve to a Dataset instance."""
    if isinstance(location, Dataset):
        if locator.match(location):
            return location
        return None
    if location.is_dir():
        for ds in iter_datasets(location):
            if locator.match(ds):
                return ds
    else:
        ds = Dataset.from_metadata(location) if sniff(location) else Dataset.from_data(location)
        if locator.match(ds):
            return ds
    return None  # pragma: no cover


def get_dataset(locator: str,
                download_dir: pathlib.Path,
                base: Optional[pathlib.Path] = None) -> Dataset:
    """
    :param locator: Dataset locator as specified in "Dataset discovery".
    :param download_dir: Directory to which to download remote data if necessary.
    :param base: Optional path relative to which local paths in `locator` must be resolved.
    """
    locator = DatasetLocator(locator)
    for resolver in get_resolvers():
        if isinstance(resolver, LocalResolver):
            # Local paths may need to be resolved relative to another path (e.g. the location of
            # a CLDF markdown document).
            res = resolver(locator.url_without_fragment, download_dir, base)
        else:
            res = resolver(locator.url_without_fragment, download_dir)
        if res:
            res = _get_dataset(locator, res)
            if res:
                return res
    raise ValueError(f'Could not resolve dataset locator {locator}')
