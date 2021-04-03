"""
Convert the information in CLDF metadata to markdown suitable for inclusion in a README
"""
from clldutils.clilib import PathType

from pycldf.util import metadata2markdown
from pycldf.cli_util import add_dataset, get_dataset


def register(parser):
    add_dataset(parser)
    parser.add_argument(
        '--rel-path',
        help='relative path to use for links to data files',
        default='./')
    parser.add_argument(
        '-o', '--out',
        type=PathType(type='file', must_exist=False),
        default=None)


def run(args):
    ds = get_dataset(args)
    res = metadata2markdown(ds, args.dataset, rel_path=args.rel_path)
    if args.out:
        args.out.write_text(res, encoding='utf8')
    else:
        print(res)
