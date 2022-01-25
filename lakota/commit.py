"""
The `Commit` class is responsible to structure the content of a
commit file. A commit is like a sorted dataframe with the following
columns: `label`, `start`, `stop`, `digest`, `length`, `closed`.

Each row represent a slice of a series: its label, the indexes at
which it starts an stops and the digests of the different columns.

So for a given commit, if we want to know all the data related to a
given series, we can simply filter on the `label` column. Furthermore
if only a part of the series is needed (for usually between two
dates), we can use `start` and `stop` to detect if a row is relevant.

Once one or more rows are identified, the digests allows to know which
files contain the data we want. We can then read and uncompress those
and instanciate a dataframe.

Let's use the command line interface to illustrate this:

```shell
$ lakota create temperature "timestamp timestamp*" "value float"
$ cat input.csv | lakota write temperature/Paris
$ cat input.csv | lakota write temperature/Brussels
```
We have create two series with the same content.


The `rev` subcomment with the `--extend` flag lists revisions and
print their content (the commits):

```shell
$ lakota rev temperature -e

Revision: 00000000000-0000000000000000000000000000000000000000.17665b9f49e-8e692971744a1222b8a7c706b31e24c8d0a22653
Date: 2020-12-15 10:27:34.302000
label    start                stop                   length  digests
-------  -------------------  -------------------  --------  -----------------------------------------------------------------------------------
Paris    2020-06-22T00:00:00  2020-06-27T00:00:00         6  8dc25a6911f09119919c2b3d177cd4430f43c73a / de46c7d97cc7dde24962e31ee5fdee1acadd114e


Revision: 17665b9f49e-8e692971744a1222b8a7c706b31e24c8d0a22653.17665ba05dd-1b188c517bb45f98669620e4822a67a81dd15b3b*
Date: 2020-12-15 10:27:38.717000
label     start                stop                   length  digests
--------  -------------------  -------------------  --------  -----------------------------------------------------------------------------------
Brussels  2020-06-22T00:00:00  2020-06-27T00:00:00         6  8dc25a6911f09119919c2b3d177cd4430f43c73a / de46c7d97cc7dde24962e31ee5fdee1acadd114e
Paris     2020-06-22T00:00:00  2020-06-27T00:00:00         6  8dc25a6911f09119919c2b3d177cd4430f43c73a / de46c7d97cc7dde24962e31ee5fdee1acadd114e
```

(revisions are documented in `lakota.changelog`)

So we can see the two commits made in the collection, they show us
different states. The first one with only the series `Paris` and the
second with both. We can use `read` with verbose flags to illustrate
file access (see comments inlined):

```shell
$ lakota -vv read temperature/Brussels
 # The first 4 storage access (1 LIST and 3 READ) are used to identify
 #  where our collection is.
LIST .lakota/00/00/000000000000000000000000000000000000 .
READ .lakota/00/00/000000000000000000000000000000000000 00000000000-0000000000000000000000000000000000000000.17665b9e79b-acf6e197ece6782a6da6e8a50bfd7d9aa3543e90
READ .lakota/6d/1d 7aa3d69158cdfdee236f3b18791204e6e308
READ .lakota/2e/0f f7b7988ee10ef47a8973afa3f9da60b6c892
 # This directory contains our collection, listing it gives us the revisions
LIST .lakota/70/33/e90fcdda169d2f5d08da17507b0c5db52029 .
 # The latest commit is read:
READ .lakota/70/33/e90fcdda169d2f5d08da17507b0c5db52029 17665b9f49e-8e692971744a1222b8a7c706b31e24c8d0a22653.17665ba05dd-1b188c517bb45f98669620e4822a67a81dd15b3b
 # Based on the commit info, we know which file to read for each column
READ .lakota/8d/c2 5a6911f09119919c2b3d177cd4430f43c73a # -> payload of timestamp column
READ .lakota/de/46 c7d97cc7dde24962e31ee5fdee1acadd114e # -> payload of value column
 # The array are then combined in a dataframe and dumped as csv
timestamp,value
2020-06-22T00:00:00,25.0
2020-06-23T00:00:00,24.0
2020-06-24T00:00:00,27.0
2020-06-25T00:00:00,31.0
2020-06-26T00:00:00,32.0
2020-06-27T00:00:00,30.0
```

Each time a write is done on a collection, a new revision file is
created, its content is a new commit. This commit is based on the
previous one, usually with an extra line or an update line (or
both). For example, if we update the `Paris` series, and print the
commits:

```shell

$ cat input-corrected.csv
2020-06-23,24.2
2020-06-24,27.9
2020-06-25,31.0
2020-06-26,32.5
2020-06-27,30.1
2020-06-28,29.2
$ cat input-corrected.csv | lakota write temperature/Paris
$ lakota rev temperature -e
[...snipped...]
Revision: 17665ba05dd-1b188c517bb45f98669620e4822a67a81dd15b3b.17665cf207b-b5ef411d8f000b5eafcdd5c58cc4c26a82ac757e*
Date: 2020-12-15 10:50:41.787000
label     start                stop                   length  digests
--------  -------------------  -------------------  --------  -----------------------------------------------------------------------------------
Brussels  2020-06-22T00:00:00  2020-06-27T00:00:00         6  8dc25a6911f09119919c2b3d177cd4430f43c73a / de46c7d97cc7dde24962e31ee5fdee1acadd114e
Paris     2020-06-22T00:00:00  2020-06-23T00:00:00         6  8dc25a6911f09119919c2b3d177cd4430f43c73a / de46c7d97cc7dde24962e31ee5fdee1acadd114e
Paris     2020-06-23T00:00:00  2020-06-28T00:00:00         6  1602a81fb4eafda5226880c7ef9145a8dada8cf0 / fda980aa244f5bef17b9feb5faa3e6532d0f815b
```

We see two lines for the `Paris` series. The first one has been
updated (stop is now `2020-06-23`) and the second one has been
appended. The next time this series is read both line will be
used except if we filter it:

```shell
$ lakota -vv read temperature/Paris --greater-than 2020-06-23
[...snipped...]
READ .lakota/70/33/e90fcdda169d2f5d08da17507b0c5db52029 17665ba05dd-1b188c517bb45f98669620e4822a67a81dd15b3b.17665cf207b-b5ef411d8f000b5eafcdd5c58cc4c26a82ac757e
READ .lakota/16/02 a81fb4eafda5226880c7ef9145a8dada8cf0
READ .lakota/fd/a9 80aa244f5bef17b9feb5faa3e6532d0f815b
timestamp,value
2020-06-23T00:00:00,24.2
2020-06-24T00:00:00,27.9
2020-06-25T00:00:00,31.0
2020-06-26T00:00:00,32.5
2020-06-27T00:00:00,30.1
2020-06-28T00:00:00,29.2
```
"""

from itertools import chain
from threading import Lock

from numcodecs import registry
from numpy import asarray, concatenate, isin, repeat, where

from .frame import Frame
from .schema import Codec, Schema
from .utils import Closed, Pool, hashed_path

__all__ = ["Commit", "Segment"]


class Commit:

    digest_codec = Codec("U")  # FIXME use better encoding
    len_codec = Codec("int")
    label_codec = Codec("str")
    closed_codec = Codec("str")  # Could be i1

    def __init__(self, schema, label, start, stop, digest, length, closed, embedded):
        assert list(digest) == list(schema)
        self.schema = schema
        self.label = label  # Array of str
        self.start = start  # Dict of Arrays
        self.stop = stop  # Dict of arrays
        self.digest = digest  # Dict of arrays
        self.length = length  # Array of int
        self.closed = closed  # Array of ("l", "r", "b", "n")
        self.embedded = embedded or {}

    @classmethod
    def one(
        cls,
        schema,
        label,
        start,
        stop,
        digest,
        length,
        closed=Closed.BOTH,
        embedded=None,
    ):
        closed = Closed.cast(closed)
        label = asarray([label])
        start = dict(zip(schema.idx, (asarray([s]) for s in start)))
        stop = dict(zip(schema.idx, (asarray([s]) for s in stop)))
        digest = dict(zip(schema, (asarray([d], dtype="U") for d in digest)))
        length = [length]
        closed = [closed.short]
        return Commit(schema, label, start, stop, digest, length, closed, embedded)

    @classmethod
    def decode(cls, schema, payload):
        msgpck = registry.codec_registry["msgpack2"]()
        data = msgpck.decode(payload)[0]
        values = {}
        # Decode starts, stops and digests
        for key in ("start", "stop", "digest"):
            key_vals = {}
            columns = schema if key == "digest" else schema.idx
            for name in columns:
                codec = cls.digest_codec if key == "digest" else schema[name].codec
                key_vals[name] = codec.decode(data[key][name])
            values[key] = key_vals

        # Decode len and labels
        values["length"] = cls.len_codec.decode(data["length"])
        values["label"] = cls.label_codec.decode(data["label"])
        values["closed"] = cls.closed_codec.decode(data["closed"])

        # Embedded data will be decoded on demand
        values["embedded"] = data.get("embedded")
        return Commit(schema, **values)

    def encode(self):
        msgpck = registry.codec_registry["msgpack2"]()
        data = {}
        # Encode starts, stops and digests
        for key in ("start", "stop", "digest"):
            columns = self.schema if key == "digest" else self.schema.idx
            key_vals = {}
            for pos, name in enumerate(columns):
                codec = (
                    self.digest_codec if key == "digest" else self.schema[name].codec
                )
                arr = getattr(self, key)[name]
                key_vals[name] = codec.encode(arr)
            data[key] = key_vals

        # Encode digests
        for name in self.schema:
            data["digest"][name] = self.digest_codec.encode(self.digest[name])

        # Encode length, closed and labels
        data["length"] = self.len_codec.encode(self.length)
        data["closed"] = self.closed_codec.encode(self.closed)
        data["label"] = self.label_codec.encode(self.label)
        # Keep only embedded data referenced by self.digest
        keep_digests = (
            set(chain.from_iterable(self.digest.values())) & self.embedded.keys()
        )
        embedded = {d: self.embedded[d] for d in sorted(keep_digests)}
        data["embedded"] = embedded
        return msgpck.encode([data])

    def split(self, label, start, stop):
        start_values = {"_label": self.label}
        start_values.update(self.start)
        stop_values = {"_label": self.label}
        stop_values.update(self.stop)
        frm_start = Frame(Schema.from_frame(start_values), start_values)
        frm_stop = Frame(Schema.from_frame(stop_values), stop_values)
        start_pos = frm_stop.index((label,) + start, right=True)
        stop_pos = frm_start.index((label,) + stop, right=False)
        return start_pos, stop_pos

    def __len__(self):
        return len(self.label)

    def at(self, pos):
        if pos < 0:
            pos = len(self) + pos
        res = {}
        for key in ("start", "stop", "digest"):
            columns = self.schema if key == "digest" else self.schema.idx
            values = getattr(self, key)
            res[key] = tuple(values[n][pos] for n in columns)

        for key in ("label", "length", "closed"):
            res[key] = getattr(self, key)[pos]
        res["embedded"] = self.embedded
        return res

    def update(self, label, start, stop, digest, length, closed="b", embedded=None):
        closed = Closed.cast(closed)
        if not start <= stop:
            raise ValueError(f"Invalid range {start} -> {stop}")
        inner = Commit.one(
            self.schema, label, start, stop, digest, length, closed, embedded
        )
        if len(self) == 0:
            return inner

        if embedded:
            self.embedded.update(embedded)

        first = (self.at(0)["label"], self.at(0)["start"])
        last = (self.at(-1)["label"], self.at(-1)["stop"])
        if (label, start) < first and (label, stop) > last:
            return inner

        start_pos, stop_pos = self.split(label, start, stop)
        # Truncate start_pos row
        head = None
        # start_pos is the result of a bisect_right, so we have to
        # check the slot on the left that may be perfect match
        start_row = None
        if start_pos > 0:
            prev_row = self.at(start_pos - 1)
            if (prev_row["label"], prev_row["stop"]) == (label, start):
                start_pos -= 1
                start_row = prev_row
        if start_row is None:
            start_row = self.at(min(start_pos, len(self) - 1))

        if (
            label == start_row["label"]
            and start_row["start"] <= start <= start_row["stop"]
        ):
            # We hit the right of an existing row
            start_row["stop"] = start
            # If closed is open/close on left, start_row become the
            # opposite on right:
            start_row["closed"] = Closed[start_row["closed"]].set_right(not closed.left)

            if (
                start_row["start"] == start_row["stop"]
                and start_row["closed"] != Closed.BOTH
            ):
                # Ignore star_row
                head = self.head(start_pos)
            else:
                head = Commit.concat(
                    self.head(start_pos),
                    Commit.one(schema=self.schema, **start_row),
                )
            # when start_row["start"] == start_row["stop"],
            # start_row stop and start are both "overshadowed" by
            # new commit

        if head is None:
            head = self.head(start_pos)

        # Truncate stop_pos row
        tail = None
        # stop_pos is the result of a bisect_left, so we have to
        # check the slot on the right that may be perfect match
        stop_row = None
        if stop_pos < len(self):
            next_row = self.at(stop_pos)
            if (next_row["label"], next_row["start"]) == (label, stop):
                stop_row = next_row
                stop_pos += 1
        if stop_row is None:
            stop_row = self.at(max(0, stop_pos - 1))

        if label == stop_row["label"] and stop_row["start"] <= stop <= stop_row["stop"]:
            # We hit the left of an existing row
            stop_row["start"] = stop
            # If closed is open/close on right, stop_row become the
            # opposite on left:
            stop_row["closed"] = Closed[stop_row["closed"]].set_left(not closed.right)

            if (
                stop_row["start"] == stop_row["stop"]
                and start_row["closed"] != Closed.BOTH
            ):
                # Ignore stop_row
                tail = self.tail(stop_pos)
            else:
                tail = Commit.concat(
                    Commit.one(schema=self.schema, **stop_row),
                    self.tail(stop_pos),
                )
            # when stop_row["start"] == stop_row["stop"],
            # stop_row stop and start are both "overshadowed" by
            # new commit
        if tail is None:
            tail = self.tail(stop_pos)
        return Commit.concat(head, inner, tail)

    def slice(self, *pos):
        slc = slice(*pos)
        schema = self.schema
        start = {name: self.start[name][slc] for name in schema.idx}
        stop = {name: self.stop[name][slc] for name in schema.idx}
        digest = {name: self.digest[name][slc] for name in schema}
        label = self.label[slc]
        length = self.length[slc]
        closed = self.closed[slc]
        return Commit(schema, label, start, stop, digest, length, closed, self.embedded)

    def head(self, pos):
        return self.slice(None, pos)

    def tail(self, pos):
        return self.slice(pos, None)

    @classmethod
    def concat(cls, commit, *other_commits):
        schema = commit.schema
        all_ci = (commit,) + other_commits
        all_ci = tuple(ci for ci in all_ci if len(ci) > 0)

        # Make sure there are no overlaps
        for prv, nxt in zip(all_ci[:-1], all_ci[1:]):
            prv_tail = prv.at(-1)
            nxt_head = nxt.at(0)
            assert (prv_tail["label"], prv_tail["stop"]) <= (
                nxt_head["label"],
                nxt_head["start"],
            )

        start = {
            name: concatenate([ci.start[name] for ci in all_ci]) for name in schema.idx
        }
        stop = {
            name: concatenate([ci.stop[name] for ci in all_ci]) for name in schema.idx
        }
        digest = {
            name: concatenate([ci.digest[name] for ci in all_ci]) for name in schema
        }
        label = concatenate([ci.label for ci in all_ci])
        length = concatenate([ci.length for ci in all_ci])
        closed = concatenate([ci.closed for ci in all_ci])
        embedded = {}
        for ci in all_ci:
            embedded.update(ci.embedded)
        return Commit(schema, label, start, stop, digest, length, closed, embedded)

    def __repr__(self):
        fmt = lambda a: "/".join(map(str, a))
        starts = list(map(fmt, zip(*self.start.values())))
        stops = list(map(fmt, zip(*self.stop.values())))
        items = "\n        ".join(
            f"{l}[{a} -> {b} ({c})]"
            for l, a, b, c in zip(self.label, starts, stops, self.closed)
        )
        return f"<Commit {items}>"

    def match(self, label):
        (matches,) = where(self.label == label)
        for pos in matches:
            yield self.at(pos)

    def segments(self, label, pod, start=None, stop=None, closed=Closed.BOTH):
        closed = Closed.cast(closed)

        # If start (or stop) is not set, and the left (right) side is
        # not closed, we ignore the very first (last) item, which
        # makes no sense
        if start is None:
            closed = closed.set_left(True)
        if stop is None:
            closed = closed.set_right(True)

        # XXX allow to pass condition instead of simple start-stop ?
        for row in self.match(label):
            arr_start = row["start"]
            arr_stop = row["stop"]
            arr_closed = Closed[row["closed"]]
            if start:
                if start > arr_stop:
                    # start is on the right of the array
                    continue
                elif not arr_closed.right and start == arr_stop:
                    # Same
                    continue
                elif start > arr_start:
                    # `closed` "win" over arr_closed on the left
                    arr_closed = arr_closed.set_left(closed.left)
                    arr_start = start
                elif start == arr_start and arr_closed.left:
                    # `closed` "win" only if array left is not already
                    # open
                    arr_closed = arr_closed.set_left(closed.left)

            if stop:
                if stop < arr_start:
                    continue
                elif not arr_closed.left and stop == arr_start:
                    continue
                elif stop < arr_stop:
                    # closed "win" over arr_closed on the right
                    arr_closed = arr_closed.set_right(closed.right)
                    arr_stop = stop
                elif stop == arr_stop and arr_closed.right:
                    arr_closed = arr_closed.set_right(closed.right)
            yield Segment(
                self,
                pod,
                row["digest"],
                start=arr_start,
                stop=arr_stop,
                closed=arr_closed,
            )

    def delete_labels(self, rm_labels):
        keep = ~isin(self.label, rm_labels)
        return self.mask(keep)

    def mask(self, keep):
        return Commit(
            schema=self.schema,
            label=self.label[keep],
            start={k: v[keep] for k, v in self.start.items()},
            stop={k: v[keep] for k, v in self.stop.items()},
            digest={k: v[keep] for k, v in self.digest.items()},
            length=self.length[keep],
            closed=self.closed[keep],
            embedded=self.embedded,
        )

    def rename_label(self, from_label, to_label):
        # First we create an extract with `from_label` only
        extract = self.mask(self.label == from_label)
        base_ci = self.delete_labels(from_label)

        # replace label in extract
        extract.label = repeat(to_label, len(extract))

        # Re-inject it
        for pos in range(len(extract)):
            row = extract.at(pos)
            base_ci = base_ci.update(**row)

        return base_ci

    def __contains__(self, row):
        start_pos, _ = self.split(row["label"], row["start"], row["stop"])
        if start_pos >= len(self):
            return False
        match_row = self.at(start_pos)
        for attr in ("start", "stop", "digest"):
            if match_row[attr] != row[attr]:
                return False
        return True


class Segment:
    def __init__(self, commit, pod, digests, start, stop, closed):
        self.commit = commit
        self.pod = pod
        self.start = start
        self.stop = stop
        self.closed = closed
        self.digest = dict(zip(commit.schema, digests))
        self._frm = None
        self.start_pos = None
        self.stop_pos = None
        self.lock = Lock()

    def __len__(self):
        return len(self.frame)

    def read(self, name, start_pos=None, stop_pos=None):
        # Prime cache
        if not name in self.frame:
            self.frame[name] = self._read(name)
        return self.frame[name][start_pos:stop_pos]

    def _read(self, name):
        dig = self.digest[name]
        # check first if content is not already in commit
        data = self.commit.embedded.get(dig)
        if data is None:
            folder, filename = hashed_path(dig)
            sub_pod = self.pod.cd(folder)
            try:
                data = sub_pod.read(filename)
            except FileNotFoundError:
                for f in sub_pod.ls():
                    # File is in soft-delete mode
                    if f.startswith(filename):
                        data = sub_pod.read(f)
                        break
                else:
                    raise RuntimeError(f'File {folder}/{filename} is missing!')

        codec = self.commit.schema[name].codec
        arr = codec.decode(data)
        return arr[self.start_pos : self.stop_pos]

    @property
    def frame(self):
        # Use a Frame instance as container to cache columns
        with self.lock:
            if self._frm is not None:
                return self._frm

            cols = {}
            # with Pool() as pool: # TODO need a smarter pool
            #     for name in self.commit.schema.idx:
            #         pool.submit(lambda: cols.update({name: self._read(name)}))
            for name in self.commit.schema.idx:
                cols[name] = self._read(name)

            frm = Frame(self.commit.schema, cols)
            self.start_pos, self.stop_pos = frm.slice_index(
                self.start, self.stop, closed=self.closed
            )
            self._frm = frm.slice(self.start_pos, self.stop_pos)
            return self._frm
