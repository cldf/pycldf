import csv
import sys
import contextlib

from clldutils.clilib import (
    register_subcommands, get_parser_and_subparsers, ParserError, add_csv_field_size_limit)
from clldutils.loglib import Logging
from termcolor import colored

import pycldf.commands


def main(args=None, catch_all=False, parsed_args=None, log=None):
    parser, subparsers = get_parser_and_subparsers('cldf')
    add_csv_field_size_limit(parser, default=csv.field_size_limit())
    register_subcommands(subparsers, pycldf.commands)

    args = parsed_args or parser.parse_args(args=args)

    if not hasattr(args, "main"):
        parser.print_help()
        return 1

    with contextlib.ExitStack() as stack:
        if not log:  # pragma: no cover
            stack.enter_context(Logging(args.log, level=args.log_level))
        else:
            args.log = log
        try:
            return args.main(args) or 0
        except KeyboardInterrupt:  # pragma: no cover
            return 0
        except ParserError as e:
            print(colored(str(e), 'red'))
            return main([args._command, '-h'])
        except Exception as e:  # pragma: no cover
            if catch_all:
                print(e)
                return 1
            raise


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main() or 0)
