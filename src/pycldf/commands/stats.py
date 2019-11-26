"""
Print basic stats for CLDF dataset <DATASET>, where <DATASET> may be the path to
- a CLDF metadata file
- a CLDF core data file
"""
from clldutils.clilib import Table, add_format

from pycldf.cli_util import add_dataset, get_dataset


def register(parser):
    add_dataset(parser)
    add_format(parser, default=None)


def run(args):
    ds = get_dataset(args)
    print(ds)
    with Table('key', 'value') as md:
        md.extend(ds.properties.items())
    print()
    with Table('Path', 'Type', 'Rows') as t:
        for p, type_, r in ds.stats():
            t.append([p, type_, r])
