"""
Recombine large media files from chunks as created by the `splitmedia` sub-command.
"""
from pycldf.cli_util import add_dataset, get_dataset
from pycldf.media import MediaTable


def register(parser):
    add_dataset(parser)


def run(args):
    ds = get_dataset(args)
    res = MediaTable(ds).cat()
    if res:
        args.log.info('{} files have been recombined'.format(res))
