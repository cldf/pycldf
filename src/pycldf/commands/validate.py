"""
Validate a dataset against the CLDF specification, i.e. check
- whether required tables and columns are present
- whether values for required columns are present
- the referential integrity of the dataset
"""
from pycldf.cli_util import add_dataset, get_dataset


def register(parser):
    add_dataset(parser)


def run(args):
    return 0 if get_dataset(args).validate(log=args.log) else 1
