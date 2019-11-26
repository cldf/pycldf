"""
Dump the contents of a SQLite DB according to the metadata of a dataset.

This allows adding data (i.e. rows) to a dataset within SQLite - maintaining schema
compatibility via the dataset metadata kept outside.
"""
from pycldf.cli_util import add_database, get_database, PathType


def register(parser):
    add_database(parser)
    parser.add_argument(
        '--metadata-path',
        help='Path to the metadata file for the output CLDF dataset',
        type=PathType(type='file'),
    )


def run(args):
    db = get_database(args)
    mdpath = args.metadata_path or db.dataset.tablegroup._fname
    args.log.info('dumped db to {0}'.format(db.to_cldf(mdpath.parent, mdname=mdpath.name)))
