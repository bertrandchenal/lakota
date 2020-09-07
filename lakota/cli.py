import argparse
import csv
import os
import sys

from tabulate import tabulate

from . import __version__
from .registry import Registry
from .schema import Schema
from .utils import logger, timeit


def get_registry(args):
    return Registry(args.uri)


def get_series(args):
    reg = get_registry(args)
    series = reg.get(args.label)
    if series is None:
        exit(f"Series '{args.label}' not found")
    return series


def read(args):
    series = get_series(args)
    columns = args.columns or list(series.schema.columns)
    query = series[columns][args.greater_than : args.less_than] @ {
        "limit": args.limit,
        "offset": args.offset,
    }

    if args.paginate:
        frames = query.paginate(args.paginate)
    else:
        frames = [query.frame()]

    if args.pretty:
        for frm in frames:
            rows = zip(*(frm[col] for col in columns))
            if len(frm) == 0:
                print(tabulate([], headers=columns))
            else:
                print(tabulate(rows, headers=columns))
    else:
        writer = csv.writer(sys.stdout)
        writer.writerow(columns)
        for frm in frames:
            rows = zip(*(frm[col] for col in columns))
            writer.writerows(rows)


def length(args):
    series = get_series(args)
    print(len(series))


def revisions(args):
    if args.label:
        series = get_series(args)
    else:
        reg = get_registry(args)
        series = reg.schema_series
    cols = ["start", "stop", "lenght"]
    rows = [(r["start"], r["stop"], r["len"]) for r in series.revisions()]
    if args.pretty:
        print(tabulate(rows, headers=cols))
    else:
        writer = csv.writer(sys.stdout)
        writer.writerow(cols)
        writer.writerows(rows)


def ls(args):
    reg = get_registry(args)
    rows = [[label] for label in reg.search()["label"]]
    if args.pretty:
        print(tabulate(rows, headers=["label"]))
    else:
        writer = csv.writer(sys.stdout)
        writer.writerow(["label"])
        writer.writerows(rows)


def create(args):
    reg = get_registry(args)
    schema = Schema(args.columns)
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


def push(args):
    reg = get_registry(args)
    remote_reg = Registry(args.remote)
    reg.push(remote_reg, *args.labels)


def pull(args):
    reg = get_registry(args)
    remote_reg = Registry(args.remote)
    reg.pull(remote_reg, *args.labels)


def pack(args):
    series = get_series(args)
    series.changelog.pack()


def truncate(args):
    series = get_series(args)
    series.truncate()


def delete(args):
    reg = get_registry(args)
    reg.delete(args.label)


def gc(args):
    reg = get_registry(args)
    cnt = reg.gc()
    print(f"{cnt} segments deleted")


def print_help(parser, args):
    parser.parse_args([args.help_cmd, "-h"])


def run():

    # Take default uri from env variable, fallback to current dir
    default_uri = os.environ.get("LAKOTA_URI", "file://.")

    # top-level parser
    parser = argparse.ArgumentParser(
        prog="lakota", formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--uri", "-u", default=default_uri, help=f"Lakota URI (default: {default_uri}"
    )
    parser.add_argument("--timing", "-t", action="store_true", help="Enable timing")
    parser.add_argument("--pretty", "-P", action="store_true", help="Tabulate output")
    parser.add_argument(
        "--verbose", "-v", action="count", help="Increase verbosity", default=0
    )
    subparsers = parser.add_subparsers(dest="command")

    # Add read command
    parser_read = subparsers.add_parser("read")
    parser_read.add_argument("label")
    parser_read.add_argument("columns", nargs="*")
    parser_read.add_argument("--limit", "-l", type=int, default=None)
    parser_read.add_argument("--offset", "-o", type=int, default=None)
    parser_read.add_argument("--paginate", "-p", type=int, default=None)
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

    # Add rev command
    parser_rev = subparsers.add_parser("rev")
    parser_rev.add_argument("label", nargs="?")
    parser_rev.set_defaults(func=revisions)

    # Add len command
    parser_len = subparsers.add_parser("ls")
    parser_len.set_defaults(func=ls)

    # Add squash command
    parser_squash = subparsers.add_parser("squash")
    parser_squash.add_argument("labels", nargs="*")
    parser_squash.set_defaults(func=squash)

    # Add push command
    parser_push = subparsers.add_parser("push")
    parser_push.add_argument("remote")
    parser_push.add_argument("labels", nargs="*")
    parser_push.set_defaults(func=push)

    # Add pull command
    parser_pull = subparsers.add_parser("pull")
    parser_pull.add_argument("remote")
    parser_pull.add_argument("labels", nargs="*")
    parser_pull.set_defaults(func=pull)

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

    # Add delete command
    parser_delete = subparsers.add_parser("delete")
    parser_delete.add_argument("label")
    parser_delete.set_defaults(func=delete)

    # Add truncate command
    parser_truncate = subparsers.add_parser("truncate")
    parser_truncate.add_argument("label")
    parser_truncate.set_defaults(func=truncate)

    # Add gc command
    parser_gc = subparsers.add_parser("gc")
    parser_gc.set_defaults(func=gc)

    # Add help command
    parser_help = subparsers.add_parser("help")
    parser_help.add_argument("help_cmd")
    parser_help.set_defaults(func=lambda args: print_help(parser, args))

    # Add version command
    parser_len = subparsers.add_parser("version")
    parser_len.set_defaults(func=lambda *a: print(__version__))

    # Parse args
    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        return
    # Enable logging
    if args.verbose == 1:
        logger.setLevel("INFO")
    elif args.verbose > 1:
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
