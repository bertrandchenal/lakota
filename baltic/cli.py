import argparse
import csv
import os
import sys

from tabulate import tabulate

from .registry import Registry
from .schema import Schema
from .utils import logger, timeit

# generated from http://www.patorjk.com/software/taag/
# With fond "Calvin S"
banner = """
┌┐ ┌─┐┬ ┌┬┐┬┌─┐
├┴┐├─┤│  │ ││
└─┘┴ ┴┴─┘┴ ┴└─┘
"""


def get_registry(args):
    return Registry(args.uri, lazy=args.lazy)


def get_series(args):
    reg = get_registry(args)
    series = reg.get(args.label)
    if series is None:
        exit(f"Series '{args.label}' not found")
    return series


def read(args):
    series = get_series(args)
    columns = args.columns or series.schema.columns
    frm = series.read(start=args.greater_than, end=args.less_than, limit=args.limit)
    if len(frm) == 0:
        print(tabulate([], headers=columns))
    else:
        rows = zip(*(frm[col] for col in columns))
        print(tabulate(rows, headers=columns))


def length(args):
    series = get_series(args)
    frm = series.read()
    print(len(frm))


def ls(args):
    reg = get_registry(args)
    rows = [[label] for label in reg.search()["label"]]
    print(tabulate(rows, headers=["label"]))


def create(args):
    reg = get_registry(args)
    schema = Schema(args.columns, idx_len=args.idx_len)
    reg.create(schema, args.label)


def write(args):
    series = get_series(args)
    reader = csv.reader(sys.stdin)
    columns = zip(*reader)
    schema = series.schema
    df = dict(zip(schema.columns, columns))
    series.write(df)


def squash(args):
    """
    Squash changelog of given series. If not series is given, squash
    registry changelog.
    """
    reg = get_registry(args)
    if args.labels:
        for label in args.labels:
            series = reg.get(label)
            series.squash()
    else:
        reg.schema_series.squash()


def pack(args):
    series = get_series(args)
    series.changelog.pack()


def clear(args):
    reg = get_registry(args)
    reg.clear()


def gc(args):
    reg = get_registry(args)
    cnt = reg.gc()
    print(f"{cnt} frames deleted")


def print_help(parser, args):
    parser.parse_args([args.help_cmd, "-h"])


def run():

    # Take default uri from env variable, fallback to current dir
    default_uri = os.environ.get("BALTIC_URI", "file://.")

    # top-level parser
    parser = argparse.ArgumentParser(
        prog="baltic",
        description=banner,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--uri", "-u", default=default_uri, help=f"Baltic URI (default: {default_uri}"
    )
    parser.add_argument("--timing", "-t", action="store_true", help="Enable timing")
    parser.add_argument("--verbose", "-v", action="count", help="Increase verbosity")
    parser.add_argument(
        "--lazy", "-L", action="store_true", help="Rely only on local cache"
    )
    subparsers = parser.add_subparsers(dest="command")

    # Add read command
    parser_read = subparsers.add_parser("read")
    parser_read.add_argument("label")
    parser_read.add_argument("columns", nargs="*")
    parser_read.add_argument("--limit", "-l", type=int, default=1000)
    parser_read.add_argument(
        "--greater-than", "--gt", nargs="+", help="Apply expression as mask"
    )
    parser_read.add_argument(
        "--less-than", "--lt", nargs="+", help="Apply expression as mask"
    )
    parser_read.set_defaults(func=read)

    # Add len command
    parser_len = subparsers.add_parser("len")
    parser_len.add_argument("label")
    parser_len.set_defaults(func=length)

    # Add len command
    parser_len = subparsers.add_parser("ls")
    parser_len.set_defaults(func=ls)

    # Add squash command
    parser_squash = subparsers.add_parser("squash")
    parser_squash.add_argument("labels", nargs="*")
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

    # Parse args
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return
    # Enable logging
    if args.verbose:
        logger.setLevel("DEBUG")

    # Execute command
    try:
        if args.timing:
            with timeit(f"Timing ({args.command}):"):
                args.func(args)
        else:
            args.func(args)

    except (BrokenPipeError, KeyboardInterrupt):
        pass
