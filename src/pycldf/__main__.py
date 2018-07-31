# coding: utf8
"""
Main command line interface of the pycldf package.

Like programs such as git, this cli splits its functionality into sub-commands
(see e.g. https://docs.python.org/2/library/argparse.html#sub-commands).
The rationale behind this is that while a lot of different tasks may be triggered using
this cli, most of them require common configuration.

The basic invocation looks like

    cldf [OPTIONS] <command> [args]

"""
from __future__ import unicode_literals, print_function
import sys

from clldutils.path import Path
from clldutils.clilib import ArgumentParserWithLogging, ParserError
from clldutils.markup import Table

from pycldf.dataset import Dataset
from pycldf.db import Database


def _get_dataset(args):
    if len(args.args) < 1:
        raise ParserError('not enough arguments')
    fname = Path(args.args[0])
    if not fname.exists() or not fname.is_file():
        raise ParserError('%s is not an existing directory' % fname)
    if fname.suffix == '.json':
        return Dataset.from_metadata(fname)
    return Dataset.from_data(fname)


def validate(args):
    """
    cldf validate <DATASET>

    Validate a dataset against the CLDF specification, i.e. check
    - whether required tables and columns are present
    - whether values for required columns are present
    - the referential integrity of the dataset
    """
    ds = _get_dataset(args)
    ds.validate(log=args.log)


def stats(args):
    """
    cldf stats <DATASET>

    Print basic stats for CLDF dataset <DATASET>, where <DATASET> may be the path to
    - a CLDF metadata file
    - a CLDF core data file
    """
    ds = _get_dataset(args)
    print(ds)
    md = Table('key', 'value')
    md.extend(ds.properties.items())
    print(md.render(condensed=False, tablefmt=None))
    print()
    t = Table('Path', 'Type', 'Rows')
    for p, type_, r in ds.stats():
        t.append([p, type_, r])
    print(t.render(condensed=False, tablefmt=None))


def createdb(args):
    """
    cldf createdb <DATASET> <SQLITE_DB_PATH>

    Load CLDF dataset <DATASET> into a SQLite DB, where <DATASET> may be the path to
    - a CLDF metadata file
    - a CLDF core data file
    """
    if len(args.args) < 2:
        raise ParserError('not enough arguments')
    db = Database(args.args[1])
    db.create()
    ds = _get_dataset(args)
    db.load(ds)
    args.log.info('{0} loaded in {1}'.format(ds, db.fname))


def main():  # pragma: no cover
    parser = ArgumentParserWithLogging('pycldf', stats, validate, createdb)
    sys.exit(parser.main())


if __name__ == "__main__":  # pragma: no cover
    main()
