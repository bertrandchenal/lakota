"""
# Example usage

Create a collection, and a first series:

```shell
$ # Create a collection "temperature" with columns "timestamp" and "value"
$ lakota create temperature "timestamp timestamp*" "value float"
$ ls .lakota/ # the default repository is the .lakota directory
00 2e 6d
$ cat input.csv # Some input data that contains some timestamps and values
2020-06-22,25
2020-06-23,24
2020-06-24,27
2020-06-25,31
2020-06-26,32
2020-06-27,30
$ # Write into a series "Brussels"
$ cat input.csv | lakota write temperature/Brussels
```

Read the data back:

```shell
$ lakota ls # list collections
collection
temperature
$ lakota ls temperature # list series
series
Brussels
$ lakota read temperature/Brussels # read series
timestamp,value
2020-06-22T00:00:00,25.0
2020-06-23T00:00:00,24.0
2020-06-24T00:00:00,27.0
2020-06-25T00:00:00,31.0
2020-06-26T00:00:00,32.0
2020-06-27T00:00:00,30.0
$ lakota -P read temperature/Brussels # with pretty-print
timestamp              value
-------------------  -------
2020-06-22T00:00:00       25
2020-06-23T00:00:00       24
2020-06-24T00:00:00       27
2020-06-25T00:00:00       31
2020-06-26T00:00:00       32
2020-06-27T00:00:00       30
```

Update the series:

```shell
$ cat input-corrected.csv # New values
2020-06-23,24.2
2020-06-24,27.9
2020-06-25,31.0
2020-06-26,32.5
2020-06-27,30.1
2020-06-28,29.2
$ cat input-corrected.csv | lakota write temperature/Brussels
$ lakota read temperature/Brussels
timestamp,value
2020-06-22T00:00:00,25.0
2020-06-23T00:00:00,24.2
2020-06-24T00:00:00,27.9
2020-06-25T00:00:00,31.0
2020-06-26T00:00:00,32.5
2020-06-27T00:00:00,30.1
2020-06-28T00:00:00,29.2
```

Show revisions and clean history:

```shell
$ lakota rev temperature  # show revisions

Revision: 00000000000-0000000000000000000000000000000000000000.176618fecf1-1b49944eecf9fd02fb13c0f0ac2e92f4e9d62620
Date: 2020-12-14 15:03:10.961000

Revision: 176618fecf1-1b49944eecf9fd02fb13c0f0ac2e92f4e9d62620.17661929034-08d4cd873f7900d89d78e589acdbf54524da45c7*
Date: 2020-12-14 15:06:03.828000

$ lakota squash temperature # squash all revisions into one
$ lakota rev temperature

Revision: 00000000000-0000000000000000000000000000000000000000.176619478d1-425a9d4eaa62dad4e560875834ed79a17238c6a6*
Date: 2020-12-14 15:08:08.913000
```

Read supports basic group-by operations and filters:

```shell
$ # compute max value on group by month
$ lakota read temperature/Brussels "(floor self.timestamp 'M')" "(max self.value)"
(floor self.timestamp 'M'),(max self.value)
2020-06-01T00:00:00,32.5
$ lakota read temperature/Brussels  --mask "(< self.value 28)"
timestamp,value
2020-06-22T00:00:00,25.0
2020-06-23T00:00:00,24.2
2020-06-24T00:00:00,27.9
```

But `--mask` and group-by/aggregate are not compatible (yet).

Built-in help:

```shell
$ lakota  --help
usage: lakota [-h] [--repo REPO] [--timing] [--pretty] [--verbose]
              {read,len,rev,ls,squash,push,pull,create,write,delete,truncate,gc,help,version}
              ...

positional arguments:
  {read,len,rev,ls,squash,push,pull,create,write,delete,truncate,gc,help,version}

optional arguments:
  -h, --help            show this help message and exit
  --repo REPO, -r REPO  Lakota repo (default: file://.lakota
  --timing, -t          Enable timing
  --pretty, -P          Tabulate output
  --verbose, -v         Increase verbosity
$ lakota  read --help
usage: lakota read [-h] [--limit LIMIT] [--offset OFFSET]
                   [--paginate PAGINATE] [--before BEFORE] [--mask MASK]
                   [--greater-than GREATER_THAN [GREATER_THAN ...]]
                   [--less-than LESS_THAN [LESS_THAN ...]]
                   label [columns [columns ...]]

positional arguments:
  label
  columns

optional arguments:
  -h, --help            show this help message and exit
  --limit LIMIT, -l LIMIT
  --offset OFFSET, -o OFFSET
  --paginate PAGINATE, -p PAGINATE
  --before BEFORE, -B BEFORE
  --mask MASK, -m MASK
  --greater-than GREATER_THAN [GREATER_THAN ...], --gt GREATER_THAN [GREATER_THAN ...]
                        Keep rows where index is bigger the given value
  --less-than LESS_THAN [LESS_THAN ...], --lt LESS_THAN [LESS_THAN ...]
                        Keep rows where index is less than given value
```

Most sub commands come with extra doc:
```
$ lakota help ls

    List collections in a repo
    ```
    $ lakota ls
    ```

    List series in a collection
    ```
    $ lakota ls my_collection
    ```
```




# Push & pull

Create two repo and write in both:

```shell
$ lakota -r repo_A create temperature "timestamp timestamp*" "value float"
$ lakota -r repo_B create temperature "timestamp timestamp*" "value float"
$ cat input.csv | lakota -r repo_A write temperature/Brussels
$ cat input-corrected.csv | lakota -r repo_B write temperature/Brussels
$ lakota -r repo_A read temperature/Brussels
timestamp,value
2020-06-22T00:00:00,25.0
2020-06-23T00:00:00,24.0
2020-06-24T00:00:00,27.0
2020-06-25T00:00:00,31.0
2020-06-26T00:00:00,32.0
2020-06-27T00:00:00,30.0
$ lakota -r repo_B read temperature/Brussels
timestamp,value
2020-06-23T00:00:00,24.2
2020-06-24T00:00:00,27.9
2020-06-25T00:00:00,31.0
2020-06-26T00:00:00,32.5
2020-06-27T00:00:00,30.1
2020-06-28T00:00:00,29.2
```

Pull and compare revisions:

```shell
$ lakota -r repo_A pull repo_B
$ lakota -r repo_A rev temperature

Revision: 00000000000-0000000000000000000000000000000000000000.17661e795b5-1b49944eecf9fd02fb13c0f0ac2e92f4e9d62620*
Date: 2020-12-14 16:38:55.797000

Revision: 00000000000-0000000000000000000000000000000000000000.17661e7bc2b-8fb3766613e5f5a9e9556178c371e0ea3695930b*
Date: 2020-12-14 16:39:05.643000
$ lakota -r repo_B rev temperature

Revision: 00000000000-0000000000000000000000000000000000000000.17661e7bc2b-8fb3766613e5f5a9e9556178c371e0ea3695930b*
Date: 2020-12-14 16:39:05.643000
```

Merge and check merged data:

```shell
$ lakota -r repo_A merge temperature
$ lakota -r repo_A rev temperature

Revision: 00000000000-0000000000000000000000000000000000000000.17661e795b5-1b49944eecf9fd02fb13c0f0ac2e92f4e9d62620
Date: 2020-12-14 16:38:55.797000

Revision: 17661e795b5-1b49944eecf9fd02fb13c0f0ac2e92f4e9d62620.17661ec08ee-08d4cd873f7900d89d78e589acdbf54524da45c7*
Date: 2020-12-14 16:43:47.438000

Revision: 00000000000-0000000000000000000000000000000000000000.17661e7bc2b-8fb3766613e5f5a9e9556178c371e0ea3695930b
Date: 2020-12-14 16:39:05.643000

Revision: 17661e7bc2b-8fb3766613e5f5a9e9556178c371e0ea3695930b.17661ec08ef-08d4cd873f7900d89d78e589acdbf54524da45c7*
Date: 2020-12-14 16:43:47.439000
$ lakota -r repo_A read temperature/Brussels
timestamp,value
2020-06-22T00:00:00,25.0
2020-06-23T00:00:00,24.2
2020-06-24T00:00:00,27.9
2020-06-25T00:00:00,31.0
2020-06-26T00:00:00,32.5
2020-06-27T00:00:00,30.1
2020-06-28T00:00:00,29.2
```

"""


import argparse
import csv
import os
import sys
from datetime import datetime

from tabulate import tabulate

from . import __version__
from .repo import Repo
from .schema import Schema
from .utils import hextime, logger, strpt, timeit


def get_repo(args):
    return Repo(args.repo)


def get_collection(repo, label):
    collection = repo / label

    if collection:
        return collection
    match = [c for c in repo if c.startswith(label)]
    if len(match) == 1:
        return repo / match[0]
    exit(f'Collection "{label}" not found')


def get_series(args):
    repo = get_repo(args)
    if not "/" in args.label:
        exit(f'Label argument should have the form "collection/series"')
    c_label, s_label = args.label.split("/", 1)
    collection = get_collection(repo, c_label)
    series = collection / s_label
    if series:
        return series
    match = [s for s in collection if s.startswith(s_label)]
    if len(match) == 1:
        return collection / match[0]
    exit(f"Series '{args.label}' not found")


def read(args):
    """
    Basic usage:
    ```
    $ lakota read my_collection/my_series
    $ lakota read my_collection/my_series --limit 10 --offset 10
    $ lakota read my_collection/my_series --greater-than 2020-01-01
    ```

    Group-by and aggregate
    ```
    $ lakota read my_collection/my_series '(floor self.timestamp "Y")' "(max self.value)"
    ```

    Explore past revisions
    ```
    $ lakota read my_collection/my_series--before  2021-01-01
    ```

    Filter results
    ```
    lakota read my_collection/my_series --mask "(< self.some_field 42)
    ```
    """
    series = get_series(args)
    reduce = False
    if not args.columns:
        columns = list(series.schema.columns)
    elif any("(" in c for c in args.columns):
        columns = list(series.schema.columns)
        reduce = True
    else:
        columns = args.columns
    before = strpt(args.before)
    before = before and hextime(before.timestamp())
    query = series[columns][args.greater_than : args.less_than] @ {
        "limit": args.limit,
        "offset": args.offset,
        "before": before,
    }
    if args.paginate:
        frames = query.paginate(args.paginate)
    else:
        frames = [query.frame()]

    if reduce:
        kw = {c: c for c in args.columns}
        columns = args.columns
        frames = (f.reduce(**kw) for f in frames)

    if args.pretty:
        for frm in frames:
            if args.mask:
                frm = frm.mask(args.mask)
                if frm.empty:
                    continue
            rows = zip(*(frm[col] for col in columns))
            if len(frm) == 0:
                print(tabulate([], headers=columns))
            else:
                print(tabulate(rows, headers=columns))
    else:
        writer = csv.writer(sys.stdout)
        writer.writerow(columns)
        for frm in frames:
            if args.mask:
                frm = frm.mask(args.mask)
                if frm.empty:
                    continue
            rows = zip(*(frm[col] for col in columns))
            writer.writerows(rows)


def length(args):
    """
    Show total length of a collection/series
    ```
    $ lakota len my_collection
    $ lakota len my_collection/my_series
    ```
    """
    if "/" in args.label:
        series = [get_series(args)]
    else:
        repo = get_repo(args)
        clc = repo / args.label
        if clc is None:
            exit(f'Collection "{args.label}" not found')
        series = [clc / name for name in clc]
    print(sum(len(s) for s in series))


def rev(args):
    """
    Show Revision
    ```
    $ lakota rev my_collection # -e for extended output
    ```

    """
    repo = get_repo(args)
    if args.label:
        collection = repo / args.label
        if collection is None:
            exit(f"Collection '{args.label}' not found")
    else:
        collection = repo.collection_series

    fmt = lambda a: " / ".join(map(str, a))
    for rev in collection.changelog.log():
        timestamp = str(datetime.fromtimestamp(int(rev.epoch, 16) / 1000))
        print(
            f"""
Revision: {rev.path}{"*" if rev.is_leaf else ""}
Date: {timestamp}"""
        )
        if not args.extended:
            continue
        ci = rev.commit(collection)
        starts = list(map(fmt, zip(*ci.start.values())))
        stops = list(map(fmt, zip(*ci.stop.values())))
        digests = list(map(fmt, zip(*ci.digest.values())))
        rows = zip(ci.label, starts, stops, ci.length, digests)
        print(tabulate(rows, headers="label start stop length digests".split()))
        print()


def ls(args):
    """
    List collections in a repo
    ```
    $ lakota ls
    ```

    List series in a collection
    ```
    $ lakota ls my_collection
    ```

    """
    repo = get_repo(args)
    if args.label:
        collection = get_collection(repo, args.label)
        header = "series"
    else:
        collection = repo
        header = "collection"

    rows = [[label] for label in collection.ls()]
    if args.pretty:
        print(tabulate(rows, headers=[header]))
    else:
        writer = csv.writer(sys.stdout)
        writer.writerow([header])
        writer.writerows(rows)


def create(args):
    """
    Create a collection, the `*` indicate the index columns
    ```
    $ lakota create my_collection "timestamp timestamp*" "value float"
    $ lakota create another_collection "timestamp timestamp*" "category str*" "value float"
    ```
    """
    repo = get_repo(args)
    collection = args.label

    schema = Schema(args.columns)
    repo.create_collection(schema, collection)


def write(args):
    """
    Write is done through stdin
    ```
    $ cat some_file.csv | lakota write my_collection/my_series
    ```
    """
    series = get_series(args)
    reader = csv.reader(sys.stdin)
    columns = zip(*reader)
    schema = series.schema
    df = dict(zip(schema.columns, columns))
    series.write(df)


def merge(args):
    """
    Merge a collection
    ```
    $ lakota merge my_collection
    ```
    """
    repo = get_repo(args)
    collection = repo / args.label
    if not collection:
        exit(f"Collection {args.label} not found")
    collection.merge()


def squash(args):
    """
    Squash changelog of given series. If not series is given, squash
    repo changelog.
    """
    repo = get_repo(args)
    if args.labels:
        for label in args.labels:
            collection = repo / label
            if not collection:
                exit(f'Collection "{label}" not found')
            collection.squash()
    else:
        repo.registry.squash()


def push(args):
    """
    Push (the local repo in `.lakota`) to a remote repo
    ```
    $ lakota push some_remote_repo
    ```

    Push `some_repo` to `another_repo`
    ```
    $ lakota -r some_repo push another_repo
    ```
    """
    reg = get_repo(args)
    remote_reg = Repo(args.remote)
    reg.push(remote_reg, *args.labels)


def pull(args):
    """
    Similar to `push`, but with direction inversed
    """
    repo = get_repo(args)
    remote_reg = Repo(args.remote)
    repo.pull(remote_reg, *args.labels)


def delete(args):
    """
    Delete a series
    ```
    $ lakota delete my_collection/my_series
    ```

    Delete a collection
    ```
    $ lakota delete my_collection
    ```
    """
    repo = get_repo(args)
    if "/" in args.label:
        collection, series = args.label.split("/", 1)
        clct = repo / collection
        clct.delete(series)
    else:
        repo.delete(args.label)


def gc(args):
    """
    Garbage-collec a repository
    ```
    $ lakota gc
    ```
    """
    repo = get_repo(args)
    cnt = repo.gc()
    print(f"{cnt} segments deleted")


def print_help(parser, args):
    cmd = args.help_cmd and globals().get(args.help_cmd)
    if cmd and cmd.__doc__:
        print(cmd.__doc__)
    parser.parse_args([args.help_cmd, "-h"])


def run():

    # Take default repo from env variable, fallback to .lakota in current dir
    default_repo = os.environ.get("LAKOTA_REPO", "file://.lakota")

    # top-level parser
    parser = argparse.ArgumentParser(
        prog="lakota",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--repo",
        "-r",
        default=default_repo,
        help=f"Lakota repo (default: {default_repo}",
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
    parser_read.add_argument("--before", "-B", default=None)
    parser_read.add_argument("--mask", "-m", type=str, default=None)
    parser_read.add_argument(
        "--greater-than",
        "--gt",
        nargs="+",
        help="Keep rows where index is bigger the given value",
    )
    parser_read.add_argument(
        "--less-than",
        "--lt",
        nargs="+",
        help="Keep rows where index is less than given value",
    )
    parser_read.set_defaults(func=read)

    # Add len command
    parser_len = subparsers.add_parser("length", aliases=["len"])
    parser_len.add_argument("label")
    parser_len.set_defaults(func=length)

    # Add rev command
    parser_rev = subparsers.add_parser("rev")
    parser_rev.add_argument("label", nargs="?")
    parser_rev.add_argument(
        "-e", "--extended", action="store_true", help="Extended output"
    )
    parser_rev.set_defaults(func=rev)

    # Add len command
    parser_ls = subparsers.add_parser("ls")
    parser_ls.add_argument("label", nargs="?")
    parser_ls.set_defaults(func=ls)

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

    # Add create command
    parser_create = subparsers.add_parser("create")
    parser_create.add_argument("label")
    parser_create.add_argument("columns", nargs="+")
    parser_create.set_defaults(func=create)

    # Add write command
    parser_write = subparsers.add_parser("write")
    parser_write.add_argument("label")
    parser_write.set_defaults(func=write)

    # Add merge command
    parser_write = subparsers.add_parser("merge")
    parser_write.add_argument("label")
    parser_write.set_defaults(func=merge)

    # Add delete command
    parser_delete = subparsers.add_parser("delete")
    parser_delete.add_argument("label", help="collection or series to delete")
    parser_delete.set_defaults(func=delete)

    # Add gc command
    parser_gc = subparsers.add_parser("gc")
    parser_gc.set_defaults(func=gc)

    # Add help command
    parser_help = subparsers.add_parser("help")
    parser_help.add_argument("help_cmd", nargs="?")
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
