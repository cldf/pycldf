"""
Functionality to use in commandline tools which need to access CLDF datasets.
"""
import argparse
import urllib.request

from clldutils.clilib import PathType, ParserError
from csvw.utils import is_url

from pycldf import Dataset, Database
from pycldf.ext import discovery

__all__ = [
    'add_dataset', 'get_dataset',
    'UrlOrPathType', 'FlagOrPathType', 'strtobool',
    'add_database', 'get_database',
    'add_catalog_spec',
]


#
# Copied from distutils.util - because we don't want to deal with deprecation warnings.
#
def strtobool(val: str) -> int:  # pragma: no cover
    """Convert a string representation of truth to true (1) or false (0).

    True values are 'y', 'yes', 't', 'true', 'on', and '1'; false values
    are 'n', 'no', 'f', 'false', 'off', and '0'.  Raises ValueError if
    'val' is anything else.
    """
    val = val.lower()
    if val in ('y', 'yes', 't', 'true', 'on', '1'):
        return 1
    if val in ('n', 'no', 'f', 'false', 'off', '0'):
        return 0
    raise ValueError(f"invalid truth value {val}")


class FlagOrPathType(PathType):  # pylint: disable=too-few-public-methods
    """
    Argument type allowing input of a path or a boolean.

    The boolean can be used to determine whether to download a file from a known location.
    """
    def __call__(self, string):
        try:
            return bool(strtobool(string))
        except ValueError:
            return super().__call__(string)


def http_head_status(url: str) -> int:  # pragma: no cover
    """Do a HEAD request for `url` to determine its status."""
    class NoRedirection(urllib.request.HTTPErrorProcessor):
        """Don't follow redirects."""
        def http_response(self, request, response):
            return response

        https_response = http_response

    opener = urllib.request.build_opener(NoRedirection)
    return opener.open(urllib.request.Request(url, method="HEAD")).status


class UrlOrPathType(PathType):  # pylint: disable=too-few-public-methods
    """Type suitable for argparse arguments, allowing input of URL or local file path."""
    def __call__(self, string: str) -> str:
        if is_url(string):
            if self._must_exist:
                sc = http_head_status(string)
                # We accept not only HTTP 200 as valid but also common redirection codes because
                # these are used e.g. for DOIs.
                if sc not in {200, 301, 302}:
                    raise argparse.ArgumentTypeError(f'URL {string} does not exist [HTTP {sc}]!')
            return string
        super().__call__(string.partition('#')[0])
        return string


def add_dataset(parser: argparse.ArgumentParser) -> None:
    """
    Adds a positional argument named `dataset` to the parser to specify a CLDF dataset.
    """
    parser.add_argument(
        'dataset',
        metavar='DATASET',
        help="Dataset locator (i.e. URL or path to a CLDF metadata file or to the data file). "
             "Resolving dataset locators like DOI URLs might require installation of third-party "
             "packages, registering such functionality using the `pycldf_dataset_resolver` "
             "entry point.",
        type=UrlOrPathType(),
    )
    parser.add_argument(
        '--download-dir',
        type=PathType(type='dir'),
        help='An existing directory to use for downloading a dataset (if necessary).',
        default=None,
    )


def get_dataset(args: argparse.Namespace) -> Dataset:
    """
    Uses the dataset specification in `args` to return a corresponding `Dataset` instance.
    """
    try:
        return discovery.get_dataset(args.dataset, download_dir=args.download_dir)
    except TypeError as e:  # pragma: no cover
        if 'PathLike' in str(e):
            raise ParserError(
                'The dataset locator may require downloading, so you should specify --download-dir'
            ) from e
        raise


def add_database(parser: argparse.ArgumentParser, must_exist: bool = True) -> None:
    """
    Add CLI arguments to specify a CLDF SQLite database.

    Retrieve in the `run` function of a command using `get_database` (see below).
    """
    add_dataset(parser)
    parser.add_argument(
        'db',
        metavar='SQLITE_DB_PATH',
        help='Path to the SQLite db file',
        type=PathType(type='file', must_exist=must_exist),
    )
    parser.add_argument('--infer-primary-keys', action='store_true', default=False)


def get_database(args: argparse.Namespace) -> Database:
    """
    Retrieve a `Database` instance based on CLI input in `args` (see `add_database`).
    """
    return Database(get_dataset(args), fname=args.db, infer_primary_keys=args.infer_primary_keys)


def add_catalog_spec(parser: argparse.ArgumentParser, name: str) -> None:
    """Add CLI arguments suitable to specify a catalog."""
    parser.add_argument(
        '--' + name,
        metavar=name.upper(),
        type=PathType(type='dir'),
        help=f'Path to repository clone of {name.capitalize()} data')
    parser.add_argument(
        f'--{name}-version',
        help=f'Version of {name.capitalize()} data to checkout',
        default=None)
