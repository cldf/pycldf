"""
Functionality to use in commandline tools which need to access CLDF datasets.
"""
import argparse

from clldutils.clilib import PathType, ParserError
from csvw.utils import is_url
import requests

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
    elif val in ('n', 'no', 'f', 'false', 'off', '0'):
        return 0
    else:
        raise ValueError("invalid truth value %r" % (val,))


class FlagOrPathType(PathType):
    def __call__(self, string):
        try:
            return bool(strtobool(string))
        except ValueError:
            return super().__call__(string)


class UrlOrPathType(PathType):
    def __call__(self, string):
        if is_url(string):
            if self._must_exist:
                sc = requests.head(string).status_code
                # We accept not only HTTP 200 as valid but also common redirection codes because
                # these are used e.g. for DOIs.
                if sc not in {200, 301, 302}:
                    raise argparse.ArgumentTypeError(
                        'URL {} does not exist [HTTP {}]!'.format(string, sc))
            return string
        super().__call__(string.partition('#')[0])
        return string


def add_dataset(parser: argparse.ArgumentParser):
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
                'The dataset locator may require downloading, so you should specify --download-dir')
        raise


def add_database(parser, must_exist=True):
    add_dataset(parser)
    parser.add_argument(
        'db',
        metavar='SQLITE_DB_PATH',
        help='Path to the SQLite db file',
        type=PathType(type='file', must_exist=must_exist),
    )
    parser.add_argument('--infer-primary-keys', action='store_true', default=False)


def get_database(args):
    return Database(get_dataset(args), fname=args.db, infer_primary_keys=args.infer_primary_keys)


def add_catalog_spec(parser, name):
    parser.add_argument(
        '--' + name,
        metavar=name.upper(),
        type=PathType(type='dir'),
        help='Path to repository clone of {0} data'.format(name.capitalize()))
    parser.add_argument(
        '--{0}-version'.format(name),
        help='Version of {0} data to checkout'.format(name.capitalize()),
        default=None)
