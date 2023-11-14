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
import typing
import pathlib
import warnings
import functools
import urllib.parse
from importlib.metadata import entry_points

from csvw.utils import is_url

from pycldf import Dataset, iter_datasets, sniff
from pycldf.util import url_without_fragment

__all__ = ['get_dataset', 'DatasetResolver']
EP = 'pycldf_dataset_resolver'

_resolvers = []


class DatasetResolver:
    """
    Virtual base class for dataset resolvers.

    :ivar priority: A number between 0 and 10, determining the call order of registered resolvers.\
    Resolvers with higher priority will be called earlier. Thus, resolvers specifying a high \
    priority should be quick in figuring out whether they apply to a locator.
    """
    priority = 5

    def __call__(self, loc: str, download_dir: pathlib.Path) \
            -> typing.Union[None, Dataset, pathlib.Path]:
        """
        :param loc: URL pointing to a place where datasets are archived.
        :param download_dir: A directory to which resolvers can download data.
        :return: Dataset resolvers may return `None` if they do not apply to `loc`, a `Dataset` \
        instance, if a candidate dataset was found, or a local path, pointing to a metadata file
        or a directory to be searched for metadata files.
        """
        raise NotImplementedError()  # pragma: no cover


class LocalResolver(DatasetResolver):
    """
    Resolves dataset locators specifying local file paths.
    """
    priority = 100

    def __call__(self, loc: str, download_dir, base: typing.Optional[pathlib.Path]) \
            -> typing.Union[None, pathlib.Path]:
        if isinstance(loc, str) and is_url(loc):
            return
        loc = pathlib.Path(loc)
        if loc.resolve() != loc and base:
            # A relative path, to be interpreted relative to base
            loc = base.resolve().joinpath(loc)
        if loc.exists():
            return loc


class GenericUrlResolver(DatasetResolver):
    """
    URL resolver which works for generic URLs provided they point to a CLDF metadata file.
    """
    priority = -1

    def __call__(self, loc, download_dir):
        if is_url(loc):
            try:
                return Dataset.from_metadata(loc)
            except:  # noqa: E722 # pragma: no cover
                raise
                pass


class DatasetLocator(str):
    @functools.cached_property
    def parsed_url(self) -> urllib.parse.ParseResult:
        return urllib.parse.urlparse(self)

    @property
    def url_without_fragment(self):
        return url_without_fragment(self.parsed_url)

    def match(self, dataset: Dataset) -> bool:
        if self.parsed_url.fragment:
            key, _, value = self.parsed_url.fragment.partition('=')
            return dataset.properties.get(key) == value if value else key in dataset.properties
        return True


def get_resolvers():
    if not _resolvers:
        eps = entry_points()
        for ep in set(eps.select(group=EP) if hasattr(eps, 'select') else eps.get(EP, [])):
            try:
                _resolvers.append(ep.load()())
            except ImportError:  # pragma: no cover
                warnings.warn('ImportError loading entry point {0.name}'.format(ep))
                continue
    return sorted(_resolvers, key=lambda res: -res.priority)


def _get_dataset(locator: DatasetLocator, location: typing.Union[None, Dataset, pathlib.Path]):
    if isinstance(location, Dataset):
        if locator.match(location):
            return location
        return
    if location.is_dir():
        for ds in iter_datasets(location):
            if locator.match(ds):
                return ds
    else:
        ds = Dataset.from_metadata(location) if sniff(location) else Dataset.from_data(location)
        if locator.match(ds):
            return ds


def get_dataset(locator: str,
                download_dir: pathlib.Path,
                base: typing.Optional[pathlib.Path] = None) -> Dataset:
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
    raise ValueError('Could not resolve dataset locator {}'.format(locator))
