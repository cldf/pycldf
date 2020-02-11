from clldutils.clilib import PathType

from pycldf import Dataset, Database


def add_dataset(parser):
    parser.add_argument(
        'dataset',
        metavar='DATASET',
        help="Dataset specification (i.e. path to a CLDF metadata file or to the data file)",
        type=PathType(type='file'),
    )


def get_dataset(args):
    if args.dataset.suffix == '.json':
        return Dataset.from_metadata(args.dataset)
    return Dataset.from_data(args.dataset)


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
