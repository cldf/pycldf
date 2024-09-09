"""
Download the media files associated with a dataset to a local directory.

Filenames will be the item's ID with a suffix added according to media type.
"""
from clldutils.clilib import PathType

from pycldf.cli_util import add_dataset, get_dataset
from pycldf.media import MediaTable


def register(parser):
    add_dataset(parser)
    parser.add_argument(
        '--use-form-id',
        help='Use the value in formReference as name of downloaded file',
        action='store_true',
        default=False)
    parser.add_argument(
        'output',
        help='Existing local directory to download the files to',
        type=PathType(type='dir'))
    parser.add_argument(
        'filters',
        help="Filter criteria for items to be downloaded specified as COLUMN=SUBSTRING",
        nargs='*',
        default=[])


def run(args):
    filters = []
    for s in args.filters:
        col, _, substring = s.partition('=')
        filters.append((col, substring))
    for item in MediaTable(get_dataset(args), args.use_form_id):
        if all(substring in item[col] for col, substring in filters):
            item.save(args.output)
