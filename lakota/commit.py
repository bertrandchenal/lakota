from numcodecs import registry
from numpy import asarray, concatenate, isin, where

from .frame import Frame
from .schema import Codec, Schema
from .utils import hashed_path


class Commit:

    digest_codec = Codec("U")  # FIXME use better encoding
    len_codec = Codec("int")
    label_codec = Codec("str")
    closed_codec = Codec("str")  # Could be i1

    def __init__(self, schema, label, start, stop, digest, length, closed):
        assert list(digest) == list(schema)
        self.schema = schema
        self.label = label  # Array of str
        self.start = start  # Dict of Arrays
        self.stop = stop  # Dict of arrays
        self.digest = digest  # Dict of arrays
        self.length = length  # Array of int
        self.closed = closed  # Array of ("l", "r", "b", None)

    @classmethod
    def one(cls, schema, label, start, stop, digest, length, closed="both"):
        label = asarray([label])
        start = dict(zip(schema.idx, (asarray([s]) for s in start)))
        stop = dict(zip(schema.idx, (asarray([s]) for s in stop)))
        digest = dict(zip(schema, (asarray([d], dtype="U") for d in digest)))
        length = [length]
        closed = [closed]
        return Commit(schema, label, start, stop, digest, length, closed)

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
        return res

    def update(self, label, start, stop, digest, length, closed="both"):
        if not start <= stop:
            raise ValueError(f"Invalid range {start} -> {stop}")
        inner = Commit.one(self.schema, label, start, stop, digest, length, closed)
        if len(self) == 0:
            return inner

        first = (self.at(0)["label"], self.at(0)["start"])
        last = (self.at(-1)["label"], self.at(-1)["stop"])
        if (label, start) <= first and (label, stop) >= last:
            return inner

        start_pos, stop_pos = self.split(label, start, stop)
        # Corner case: we hit right in the middle of an existing row
        if start_pos + 1 == stop_pos:
            row = self.at(start_pos)
            if label == row["start"] and row["start"] < start and stop < row["stop"]:
                start_row = row
                start_row["stop"] = start
                start_row["closed"] = (
                    "left" if start_row["closed"] in ("left", "both") else None
                )
                stop_row = self.at(start_pos)
                stop_row["start"] = stop
                stop_row["closed"] = (
                    "right" if stop_row["closed"] in ("right", "both") else None
                )
                ci = Commit.concat(
                    self.head(start_pos),
                    Commit.one(schema=self.schema, **start_row),
                    inner,
                    Commit.one(schema=self.schema, **stop_row),
                    self.tail(stop_pos),
                )
                return ci

        # Truncate start_pos row
        head = None
        if start_pos < len(self):
            start_row = self.at(start_pos)
            if (
                label == start_row["label"]
                and start_row["start"] < start <= start_row["stop"]
            ):
                # We hit the right of an existing row
                start_row["stop"] = start
                # XXX adapt behaviour if current update is not closed==both
                start_row["closed"] = (
                    "left" if start_row["closed"] in ("left", "both") else None
                )
                if start_row["start"] < start_row["stop"]:
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
        stop_row = self.at(stop_pos - 1)
        if label == stop_row["label"] and stop_row["start"] <= stop < stop_row["stop"]:
            # We hit the left of an existing row
            stop_row["start"] = stop
            # XXX adapt behavoour if current update is not closed==both
            stop_row["closed"] = (
                "right" if stop_row["closed"] in ("right", "both") else None
            )
            if stop_row["start"] < stop_row["stop"]:
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
        return Commit(schema, label, start, stop, digest, length, closed)

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

        return Commit(schema, label, start, stop, digest, length, closed)

    def __repr__(self):
        fmt = lambda a: "/".join(map(str, a))
        starts = list(map(fmt, zip(*self.start.values())))
        stops = list(map(fmt, zip(*self.stop.values())))
        items = "\n        ".join(
            f"{l}[{a} -> {b}]" for l, a, b in zip(self.label, starts, stops)
        )
        return f"<Commit {items}>"

    def segments(self, label, pod, start=None, stop=None):
        res = []
        (matches,) = where(self.label == label)
        for pos in matches:
            arr_start = tuple(arr[pos] for arr in self.start.values())
            arr_stop = tuple(arr[pos] for arr in self.stop.values())
            if start and start > arr_stop:
                continue
            if stop and stop < arr_start:
                continue

            digest = [arr[pos] for arr in self.digest.values()]
            closed = self.closed[pos]
            sgm = Segment(
                self.schema,
                pod,
                digest,
                start=max(arr_start, start) if start else arr_start,
                stop=min(arr_stop, stop) if stop else arr_stop,
                closed=closed,
            )
            res.append(sgm)
        return res

    def delete_labels(self, rm_labels):
        keep = ~isin(self.label, rm_labels)
        return Commit(
            schema=self.schema,
            label=self.label[keep],
            start={k: v[keep] for k, v in self.start.items()},
            stop={k: v[keep] for k, v in self.stop.items()},
            digest={k: v[keep] for k, v in self.digest.items()},
            length=self.length[keep],
            closed=self.closed[keep],
        )

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
    def __init__(self, schema, pod, digests, start, stop, closed):
        self.schema = schema
        self.pod = pod
        self.start = start
        self.stop = stop
        self.closed = closed
        self.digest = dict(zip(schema, digests))
        self._frm = None
        self.start_pos = None
        self.stop_pos = None

    def __len__(self):
        return len(self.frame)

    def read(self, name, start=None, stop=None):
        if not name in self.frame:
            self.frame[name] = self._read(name)
        return self.frame[name][start:stop]

    def _read(self, name):
        folder, filename = hashed_path(self.digest[name])
        payload = self.pod.cd(folder).read(filename)
        arr = self.schema[name].codec.decode(payload)
        return arr[self.start_pos : self.stop_pos]

    @property
    def frame(self):
        # Use a Frame instance as container to cache columns
        if self._frm is not None:
            return self._frm

        cols = {}
        for name in self.schema.idx:
            cols[name] = self._read(name)
        frm = Frame(self.schema, cols)
        self.start_pos, self.stop_pos = frm.index_slice(
            self.start, self.stop, closed=self.closed
        )
        self._frm = frm.slice(self.start_pos, self.stop_pos)
        return frm
