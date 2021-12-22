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
    ...
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
import json
import os
import sys
from datetime import datetime
from itertools import chain

from tabulate import tabulate

from . import __version__
from .pod import POD
from .repo import Repo
from .schema import Schema
from .utils import logger, strpt, timeit

# Take default repo from env variable, fallback to .lakota in current dir
default_repo = os.environ.get("LAKOTA_REPO", "file:///.lakota")


def get_repo(args):
    return Repo(args.repo or default_repo)


def get_collection(repo, label):
    collection = repo / label

    if collection:
        return collection
    match = [c for c in repo.ls() if c.startswith(label)]
    if len(match) == 1:
        return repo / match[0]
    exit(f'Collection "{label}" not found')


def get_series(repo, label, auto_create=False):
    if not "/" in label:
        exit('Label argument should have the form "collection/series"')
    c_label, s_label = label.split("/", 1)
    collection = get_collection(repo, c_label)
    if auto_create or label in collection:
        return collection / s_label
    match = [s for s in collection.ls() if s.startswith(s_label)]
    if len(match) == 1:
        return collection / match[0]
    elif s_label in match:
        return collection / s_label
    exit(f"Series '{label}' not found")


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
    repo = get_repo(args)
    series = get_series(repo, args.label)

    reduce = False
    if not args.columns:
        columns = list(series.schema.columns)
    elif any("(" in c for c in args.columns):
        columns = list(series.schema.columns)
        reduce = True
    else:
        columns = args.columns

    kw = {
        "start": args.greater_than,
        "stop": args.less_than,
        "limit": args.limit,
        "offset": args.offset,
        "before": args.before,
        "select": columns,
        "closed": args.closed,
    }
    if args.paginate:
        frames = series.paginate(args.paginate, **kw)
    elif args.tail:
        frames = [series.tail(args.tail, **kw)]
    else:
        frames = [series.frame(**kw)]

    if args.mask:
        frames = (frm.mask(args.mask) for frm in frames)

    if reduce:
        kw = {c: c for c in args.columns}
        frames = (f.reduce(**kw) for f in frames)
        # Peek at first frame to get the colums
        first = next(frames)
        columns = list(first)
        frames = chain([first], frames)

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


def export(args):
    repo = get_repo(args)
    repo.export_collections(args.uri, args.collection, args.file_type)


def import_(args):
    repo = get_repo(args)
    repo.import_collections(args.uri, args.collection)


def length(args):
    """
    Show total length of a collection/series
    ```
    $ lakota len my_collection
    $ lakota len my_collection/my_series
    ```
    """
    repo = get_repo(args)
    label = args.label
    if "/" in args.label:
        series = [get_series(repo, label)]
    else:
        repo = get_repo(args)
        clc = get_collection(repo, label)
        if clc is None:
            exit(f'Collection "{label}" not found')
        series = list(clc)
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
        if "/" in args.label:
            series = get_series(repo, args.label)
            collection = series.collection
        else:
            series = None
            collection = get_collection(repo, args.label)
        if collection is None:
            exit(f"Collection '{args.label}' not found")
    else:
        collection = repo.collection_series
        series = None
    fmt = lambda a: " / ".join(map(str, a))
    for rev in collection.changelog.log():
        timestamp = str(datetime.fromtimestamp(int(rev.epoch, 16) / 1000))
        ci = rev.commit(collection)
        print(
            f"""
Revision: {rev.path}{"*" if rev.is_leaf else ""}
Date: {timestamp}
Total length: {sum(ci.length)}
"""
        )
        if not args.extended:
            continue
        if series is not None:
            ci = ci.mask(ci.label == series.label)
        starts = list(map(fmt, zip(*ci.start.values())))
        stops = list(map(fmt, zip(*ci.stop.values())))
        headers = ["label", "start", "stop", "length", "closed"]
        columns = [ci.label, starts, stops, ci.length, ci.closed]
        if args.extended > 1:
            digests = list(map(fmt, zip(*ci.digest.values())))
            headers.append("digests")
            columns.append(digests)

        rows = zip(*columns)
        print(tabulate(rows, headers=headers))
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

    columns = dict(col.split(maxsplit=1) for col in args.columns)
    schema = Schema(**columns)
    repo.create_collection(schema, collection)


def write(args):
    """
    Write is done through stdin
    ```
    $ cat some_file.csv | lakota write my_collection/my_series
    ```
    """
    repo = get_repo(args)
    series = get_series(repo, args.label, auto_create=True)
    reader = csv.reader(sys.stdin)
    headers = next(reader)
    columns = list(zip(*reader))
    df = dict(zip(headers, columns))
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


def defrag(args):
    """
    Defrag changelog of given series. If no series is given, defrag
    repo changelog.
    """
    repo = get_repo(args)
    labels = repo.ls() if args.all else args.labels

    if labels:
        for label in labels:
            collection = get_collection(repo, label)
            if not collection:
                exit(f'Collection "{label}" not found')
            collection.defrag()
    if args.all or not args.labels:
        repo.registry.defrag()


def trim(args):
    """
    Trim changelog of given series. If no series is given, trim
    repo changelog.
    """
    repo = get_repo(args)
    labels = repo.ls() if args.all else args.labels

    if labels:
        for label in labels:
            collection = get_collection(repo, label)
            if not collection:
                exit(f'Collection "{label}" not found')
            collection.trim(args.before)
    if args.all or not args.labels:
        repo.registry.trim(args.before)


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
    repo = get_repo(args)
    remote_repo = Repo(args.remote)
    repo.push(remote_repo, *args.labels, shallow=args.shallow)


def pull(args):
    """
    Similar to `push`, but with direction inversed
    """
    repo = get_repo(args)
    remote_reg = Repo(args.remote)
    repo.pull(remote_reg, *args.labels, shallow=args.shallow)


def delete(args):
    """
    Delete a series or a collection
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
        srs = get_series(args.label)
        srs.collection.delete(srs.label)
    else:
        clc = get_collection(repo, args.label)
        repo.delete(clc.label)


def gc(args):
    """
    Garbage-collec a repository
    ```
    $ lakota gc
    ```
    """
    repo = get_repo(args)
    hard, _ = repo.gc()
    print(f"{hard} segments deleted")


def serve(args):
    try:
        from lakota import server
    except ImportError:
        raise
        exit("Please install flask to run server")

    repo_map = {}
    for item in args.repo_map:
        name, *uris = item.split()
        if not uris:
            raise ValueError("Missing uri in repo-map argument")
        repo_map[name] = uris
    server.run(repo_map, args.web_uri, debug=args.verbose)


def deploy(args):
    try:
        from lakota.aws_utils import deploy_lambda
    except ImportError:
        exit("Please install boto3 and aws-wsgi to deploy lambda")
    deploy_lambda(args.name, args.arn, args.lakota_package)


def print_help(parser, args):
    cmd = args.help_cmd and globals().get(args.help_cmd)
    if cmd and cmd.__doc__:
        print(cmd.__doc__)
    parser.parse_args([args.help_cmd, "-h"])


def bool_like(v):
    v = v.lower()
    if v in ("yes", "true", "t", "y", "1"):
        return True
    elif v in ("no", "false", "f", "n", "0"):
        return False
    raise argparse.ArgumentTypeError("Boolean value expected.")


def datetime_like(v):
    try:
        return strpt(v)
    except:
        raise argparse.ArgumentTypeError("Datetime-like value expected.")


def run():

    # top-level parser
    parser = argparse.ArgumentParser(
        prog="lakota",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--repo",
        "-r",
        action="append",
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
    parser_read.add_argument("--tail", "-t", type=int, default=None)
    parser_read.add_argument("--before", "-B", default=None, type=datetime_like)
    parser_read.add_argument("--mask", "-m", type=str, default=None)
    parser_read.add_argument(
        "--greater-than",
        "--gt",
        action="append",
        help="Keep rows where index is bigger the given value",
    )
    parser_read.add_argument(
        "--less-than",
        "--lt",
        action="append",
        help="Keep rows where index is less than given value",
    )
    parser_read.add_argument(
        "--closed",
        "-c",
        type=str,
        default="LEFT",
        help="Include or exclude the bounds of interval defined by --gt"
        ' and --lt (defaults to "LEFT" or "l", other possible values: '
        "RIGHT, r, BOTH, b and NONE, n)",
    )
    parser_read.set_defaults(func=read)

    # Add export command
    parser_export = subparsers.add_parser("export")
    parser_export.add_argument("uri", help="Where to save the export")
    parser_export.add_argument(
        "--collection",
        "-c",
        action="append",
        help="Export only the given collection(s)",
    )
    parser_export.add_argument(
        "--file-type", "-T", default="csv", help="File type: csv (default) or parquet "
    )
    parser_export.set_defaults(func=export)

    parser_import = subparsers.add_parser("import")
    parser_import.add_argument("uri", help="From where to import collections")
    parser_import.add_argument(
        "--collection",
        "-c",
        action="append",
        help="Import only the given collection(s)",
    )
    parser_import.set_defaults(func=import_)

    # Add len command
    parser_len = subparsers.add_parser("length", aliases=["len"])
    parser_len.add_argument("label")
    parser_len.set_defaults(func=length)

    # Add rev command
    parser_rev = subparsers.add_parser("rev")
    parser_rev.add_argument("label", nargs="?")
    parser_rev.add_argument(
        "-e", "--extended", action="count", default=0, help="Extended output"
    )
    parser_rev.set_defaults(func=rev)

    # Add len command
    parser_ls = subparsers.add_parser("ls")
    parser_ls.add_argument("label", nargs="?")
    parser_ls.set_defaults(func=ls)

    # Add defrag command
    parser_defrag = subparsers.add_parser("defrag")
    parser_defrag.add_argument("labels", nargs="*")
    parser_defrag.add_argument(
        "-a", "--all", action="store_true", help="Defrag all collections"
    )
    parser_defrag.set_defaults(func=defrag)

    # Add trim command
    parser_trim = subparsers.add_parser("trim")
    parser_trim.add_argument("labels", nargs="*")
    parser_trim.add_argument(
        "-b",
        "--before",
        type=datetime_like,
        help="Delete revisions older than given date",
    )
    parser_trim.add_argument("-a", "--all", action="store_true", help="Trim everything")
    parser_trim.set_defaults(func=trim)

    # Add push command
    parser_push = subparsers.add_parser("push")
    parser_push.add_argument("remote")
    parser_push.add_argument(
        "labels", nargs="*", help="Collection to push (all if not set)"
    )
    parser_push.add_argument(
        "-s",
        "--shallow",
        action="store_true",
        help="Shallow push (send only last revision)",
    )
    parser_push.set_defaults(func=push)

    # Add pull command
    parser_pull = subparsers.add_parser("pull")
    parser_pull.add_argument("remote")
    parser_pull.add_argument(
        "labels", nargs="*", help="Collection to pull (all if not set)"
    )
    parser_pull.add_argument(
        "-s",
        "--shallow",
        action="store_true",
        help="Shallow pull (fetch only last revision)",
    )
    parser_pull.set_defaults(func=pull)

    # Add create command
    parser_create = subparsers.add_parser("create")
    parser_create.add_argument("label")
    parser_create.add_argument("columns", nargs="+")
    parser_create.set_defaults(func=create)

    # Add write command
    parser_write = subparsers.add_parser("write")
    parser_write.add_argument("label")
    # TODO add --update flag to do Series.update instead of Series.write
    parser_write.set_defaults(func=write)

    # Add merge command
    parser_write = subparsers.add_parser("merge")
    parser_write.add_argument("label")
    parser_write.set_defaults(func=merge)

    # Add delete command
    parser_delete = subparsers.add_parser("delete", aliases=["del"])
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

    # Add serve command
    parser_serve = subparsers.add_parser("serve")
    parser_serve.add_argument(
        "repo_map",
        nargs="+",
        metavar="repo-map",
        help="space-separated mapping of name and repo uri. "
        "Example: 'my-local-repo file://.lakota' 'a-remote-one http://host/foobar'. "
        "Use '/' as name to serve repo at root.",
    )
    parser_serve.add_argument(
        "-w",
        "--web-uri",
        nargs="?",
        default="http://127.0.0.1:8080",
        help="Base url at which the repo will be served",
    )
    parser_serve.set_defaults(func=serve)

    # Add deploy command
    parser_deploy = subparsers.add_parser("deploy")
    parser_deploy.add_argument("name", help="Lambda function name")
    parser_deploy.add_argument("--arn", help="ARN of the role")
    parser_deploy.add_argument(
        "--lakota-package",
        help="Full path to clone (if not set use the offical package",
        default="lakota",
    )
    parser_deploy.set_defaults(func=deploy)

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
