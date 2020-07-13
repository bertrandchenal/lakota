import argparse
import csv
import sys

from tabulate import tabulate

from .registry import Registry
from .schema import Schema
from .utils import timeit

# generated from http://www.patorjk.com/software/taag/
# With fond "Calvin S"
banner = """
┌┐ ┌─┐┬ ┌┬┐┬┌─┐
├┴┐├─┤│  │ ││
└─┘┴ ┴┴─┘┴ ┴└─┘
"""


def read(args):
    reg = Registry(args.path)
    series = reg.get(args.label)
    columns = args.columns or series.schema.columns
    sgm = series.read(start=args.greater_than, end=args.less_than, limit=args.limit)
    arrays = []
    for column in columns:
        arr = sgm[column][: args.limit]
        arrays.append(arr)
    arr = arrays[1]
    rows = zip(*arrays)
    print(tabulate(rows, headers=columns))


def lenght(args):
    reg = Registry(args.path)
    series = reg.get(args.label)
    sgm = series.read()
    print(len(sgm))


def ls(args):
    reg = Registry(args.path)
    rows = [[label] for label in sorted(set(reg.search()["label"]))]
    print(tabulate(rows, headers=["label"]))


def create(args):
    reg = Registry(args.path)
    schema = Schema(args.columns, idx_len=args.idx_len)
    reg.create(schema, args.label)


def write(args):
    reg = Registry(args.path)
    series = reg.get(args.label)
    reader = csv.reader(sys.stdin)
    columns = zip(*reader)
    schema = series.schema
    df = dict(zip(schema.columns, columns))
    series.write(df)


def squash(args):
    reg = Registry(args.path)
    series = reg.get(args.label)
    series.squash()


def pack(args):
    reg = Registry(args.path)
    series = reg.get(args.label)
    series.changelog.pack()


def clear(args):
    reg = Registry(args.path)
    reg.clear()


def gc(args):
    reg = Registry(args.path)
    cnt = reg.gc()
    print(f"{cnt} segments deleted")


def print_help(parser, args):
    parser.parse_args([args.help_cmd, "-h"])


def run():

    # top-level parser
    parser = argparse.ArgumentParser(
        prog="baltic",
        description=banner,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--path", "-p", default="file://.")
    parser.add_argument("--timing", "-t", action="store_true", help="Enable timing")
    subparsers = parser.add_subparsers(dest="command")

    # Add read command
    parser_read = subparsers.add_parser("read")
    parser_read.add_argument("label")
    parser_read.add_argument("columns", nargs="*")
    parser_read.add_argument("--limit", "-l", type=int, default=1000)
    parser_read.add_argument("--greater-than", "--gt", nargs="+", help="Apply expression as mask")
    parser_read.add_argument("--less-than", "--lt", nargs="+", help="Apply expression as mask")
    parser_read.set_defaults(func=read)

    # Add len command
    parser_len = subparsers.add_parser("len")
    parser_len.add_argument("label")
    parser_len.set_defaults(func=lenght)

    # Add len command
    parser_len = subparsers.add_parser("ls")
    parser_len.set_defaults(func=ls)

    # Add squash command
    parser_squash = subparsers.add_parser("squash")
    parser_squash.add_argument("label")
    parser_squash.set_defaults(func=squash)

    # Add pack command
    parser_pack = subparsers.add_parser("pack")
    parser_pack.add_argument("label")
    parser_pack.set_defaults(func=pack)

    # Add create command
    parser_create = subparsers.add_parser("create")
    parser_create.add_argument("label")
    parser_create.add_argument("columns", nargs="+")
    parser_create.add_argument("--idx-len", type=int)
    parser_create.set_defaults(func=create)

    # Add write command
    parser_write = subparsers.add_parser("write")
    parser_write.add_argument("label")
    parser_write.set_defaults(func=write)

    # Add clear command
    parser_clear = subparsers.add_parser("clear")
    parser_clear.set_defaults(func=clear)

    # Add gc command
    parser_gc = subparsers.add_parser("gc")
    parser_gc.set_defaults(func=gc)

    # Add help command
    parser_help = subparsers.add_parser("help")
    parser_help.add_argument("help_cmd")
    parser_help.set_defaults(func=lambda args: print_help(parser, args))

    # Execute command
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return
    try:
        if args.timing:
            with timeit(f"Timing ({args.command}):"):
                args.func(args)
        else:
            args.func(args)

    except (BrokenPipeError, KeyboardInterrupt):
        pass
