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
    print()
    t = Table('Path', 'Type', 'Rows')
    for p, type_, r in ds.stats():
        t.append([p, type_, r])
    print(t.render(condensed=False, tablefmt=None))


def main():  # pragma: no cover
    parser = ArgumentParserWithLogging('pycldf', stats, validate)
    sys.exit(parser.main())
