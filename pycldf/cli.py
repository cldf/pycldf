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
from clldutils.clilib import ArgumentParser, ParserError
from clldutils.jsonlib import load

from pycldf.metadata import Metadata
from pycldf.util import MD_SUFFIX


def datasets(args):
    """
    cldf datasets <DIR> [ATTRS]

    List all CLDF datasets in directory <DIR>
    """
    if len(args.args) < 1:
        raise ParserError('not enough arguments')
    d = Path(args.args[0])
    if not d.exists() or not d.is_dir():
        raise ParserError('%s is not an existing directory' % d)
    for fname in sorted(d.glob('*' + MD_SUFFIX), key=lambda p: p.name):
        md = Metadata(load(fname))
        data = fname.parent.joinpath(
            md.get_table().url or fname.name[:-len(MD_SUFFIX)])
        if data.exists():
            print(data)
            if len(args.args) > 1:
                maxlen = max(len(a) for a in args.args[1:])
                for attr in args.args[1:]:
                    if md.get(attr):
                        print('    %s %s' % ((attr + ':').ljust(maxlen + 1), md[attr]))


def main():  # pragma: no cover
    parser = ArgumentParser('pycldf', datasets)
    sys.exit(parser.main())
