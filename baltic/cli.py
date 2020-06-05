from .registry import Registry
from .utils import timeit

import argparse


def read(args):
    reg = Registry(args.path)
    series = reg.get(args.label)
    columns = args.columns or series.schema.columns
    sgm = series.read()
    for column in columns:
        arr = sgm[column][:]
        if args.head:
            arr = arr[:args.head]
        if args.tail:
            arr = arr[-args.tail:]
        print(column)
        for item in arr:
            print('\t', item)


def lenght(args):
    reg = Registry(args.path)
    series = reg.get(args.label)
    sgm = series.read()
    print(len(sgm))


def squash(args):
    reg = Registry(args.path)
    series = reg.get(args.label)
    series.squash()

def pack(args):
    reg = Registry(args.path)
    series = reg.get(args.label)
    series.changelog.pack()


def tree(args):
    reg = Registry(args.path)
    series = reg.get(args.label)
    print(series.group.tree())


def print_help(parser, args):
    parser.parse_args([args.command, '-h'])


def run():
    # top-level parser
    parser = argparse.ArgumentParser(prog='baltic')
    parser.add_argument('--path', '-p', default='.')
    parser.add_argument('--timing', '-t', action='store_true',
                        help='Enable timing')
    subparsers = parser.add_subparsers(dest='command')

    # Add read command
    parser_read = subparsers.add_parser('read')
    parser_read.add_argument('label')
    parser_read.add_argument('columns', nargs='*')
    parser_read.add_argument('--head', '-H', type=int)
    parser_read.add_argument('--tail', '-T', type=int)
    parser_read.set_defaults(func=read)

    # Add len command
    parser_len = subparsers.add_parser('len')
    parser_len.add_argument('label')
    parser_len.set_defaults(func=lenght)

    # Add squash command
    parser_squash = subparsers.add_parser('squash')
    parser_squash.add_argument('label')
    parser_squash.set_defaults(func=squash)

    # Add pack command
    parser_pack = subparsers.add_parser('pack')
    parser_pack.add_argument('label')
    parser_pack.set_defaults(func=pack)

    # Add tree command
    parser_tree = subparsers.add_parser('tree')
    parser_tree.add_argument('label')
    parser_tree.set_defaults(func=tree)

    # Add help command
    parser_help = subparsers.add_parser('help')
    parser_help.add_argument('command')
    parser_help.set_defaults(func=lambda args: print_help(parser, args))

    # Execute command
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return
    try:
        if args.timing:
            with timeit(f'Timing ({args.command}):'):
                args.func(args)
        else:
            args.func(args)

    except (BrokenPipeError, KeyboardInterrupt):
        pass
