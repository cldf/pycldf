"""
Load a CLDF dataset into a SQLite DB
"""
from clldutils.clilib import ParserError

from pycldf.cli_util import add_database, get_database


def register(parser):  # pylint: disable=C0116
    add_database(parser, must_exist=False)


def run(args):  # pylint: disable=C0116
    if args.db.exists():
        raise ParserError('The database file already exists!')
    db = get_database(args)
    db.write_from_tg()
    args.log.info('%s loaded in %s', db.dataset, db.fname)
