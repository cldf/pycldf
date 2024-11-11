"""
Split large media files in a dataset into chunks.

The chunks created by `splitmedia` can be recombined using the `catmedia` sub-command.
"""
import re
import argparse

from pycldf.cli_util import add_dataset, get_dataset
from pycldf.media import MediaTable

# By default, we split into 50MB chunks.
CHUNKSIZE = 50 * 1000 * 1000


class Bytes:
    def __call__(self, string):
        if not re.fullmatch(r'[0-9]+([MK])?', string):  # pragma: no cover
            raise argparse.ArgumentTypeError('Invalid chunksize {0}!'.format(string))
        return eval(string.replace('K', '*1024').replace('M', '*1024*1024'))


def register(parser):
    add_dataset(parser)
    parser.add_argument(
        '-b', '--bytes',
        metavar='SIZE',
        help='The SIZE argument is an integer and optional unit K or M (example: 10K is 10*1024).',
        type=Bytes(),
        default=CHUNKSIZE,
    )


def run(args):
    ds = get_dataset(args)
    res = MediaTable(ds).split(args.bytes)
    if res:
        args.log.info('{} files have been split'.format(res))
