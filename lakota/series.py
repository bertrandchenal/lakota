from time import time

from numcodecs import registry
from numpy import arange, concatenate, issubdtype, where

from .changelog import phi
from .frame import Frame, ShallowSegment
from .schema import Codec, Schema
from .utils import (
    Interval,
    Pool,
    binhash_len,
    default_hash,
    encoder,
    hashed_path,
    hexdigest,
)


def intersect(revision, start, stop):
    ok_start = not stop or revision["start"][: len(stop)] <= stop
    ok_stop = not start or revision["stop"][: len(start)] >= start
    if not (ok_start and ok_stop):
        return None
    # return reduced range
    max_start = max(revision["start"], start)
    min_stop = min(revision["stop"], stop) if stop else revision["stop"]
    return (max_start, min_stop)


class Series:
    """
    Combine a pod and a changelog to provide a versioned and
    concurrent management of series.
    """

    def __init__(self, label, collection):
        self.collection = collection
        self.schema = collection.schema
        self.pod = collection.pod
        self.changelog = collection.changelog
        self.label = label

    # def revisions(self):
    #     fltr = lambda rev: rev["label"] == self.label
    #     return list(self.changelog.walk(fltr))

    def refresh(self):
        self.changelog.refresh()

    def segments(
        self,
        start=None,
        stop=None,
        after=None,
        before=None,
        closed="left",
    ):
        """
        Find matching segments
        """
        # TODO filter on before/after
        leaf_rev = self.changelog.leaf()
        if not leaf_rev:
            return

        payload = leaf_rev.read()
        leaf_ci = Commit.decode(self.schema, payload)
        # TODO filter on start and stop
        return leaf_ci.segments(self.label, self.pod)

    def period(self, rev):
        """
        Return average period (time delta between two tic) of a given revision
        """
        start = self.schema.deserialize(rev["start"])[0]
        stop = self.schema.deserialize(rev["stop"])[0]
        span = stop - start
        # span is a timedelta64
        span = span.item().total_seconds()
        return span / rev["len"]

    def interval(self, size=500_000):
        """
        Find smallest natural partition that will fit `size` items
        """
        schema = self.schema
        head_col = next(iter(schema.idx))
        assert issubdtype(schema[head_col].dt, "datetime64")

        revisions = self.revisions()
        if not revisions:
            return None
        min_period = min(self.period(rev) for rev in self.revisions())
        target = min_period * size
        return Interval.bisect(target)

    # def _read_segments(self, revisions, start, stop, closed="left"):
    #     for pos, revision in enumerate(revisions):
    #         match = intersect(revision, start, stop)
    #         if not match:
    #             continue
    #         mstart, mstop = match
    #         clsd = closed
    #         if closed == "right" and mstart > start:
    #             clsd = "both"
    #         elif closed == None and mstart > start:
    #             clsd = "left"
    #         if clsd == "left" and (mstop < stop or not stop):
    #             clsd = "both"
    #         elif clsd == None and (mstop < stop or not stop):
    #             clsd = "right"

    #         # instanciate frame
    #         sgm = revision.segment(self).slice(mstart, mstop, clsd)
    #         yield sgm

    #         # We have found one result and the search range is
    #         # collapsed, stop recursion:
    #         if len(start) and start == stop:
    #             return

    #         # recurse left
    #         if mstart > start:
    #             if closed == "both":
    #                 clsd = "left"
    #             elif closed == "right":
    #                 clsd = None
    #             else:
    #                 clsd = closed
    #             left_frm = self._read_segments(
    #                 revisions[pos + 1 :], start, mstart, closed=clsd
    #             )
    #             yield from left_frm
    #         # recurse right
    #         if not stop or mstop < stop:
    #             if closed == "both":
    #                 clsd = "right"
    #             elif closed == "left":
    #                 clsd = None
    #             else:
    #                 clsd = closed
    #             right_frm = self._read_segments(
    #                 revisions[pos + 1 :], mstop, stop, closed=clsd
    #             )
    #             yield from right_frm
    #         break

    def delete(self, root=False, batch=False):
        rev_info = {
            "tombstone": True,
            "epoch": time(),
            "label": self.label,
        }
        key = hexdigest(*encoder(self.label, "tombstone"))
        if batch:
            batch.append(rev_info, key)

        force_parent = phi if root else None
        commit = self.changelog.commit(rev_info, key=key, force_parent=force_parent)
        return commit

    def write(self, frame, start=None, stop=None, root=False, batch=False):
        # Each commit is a frame. A row in this frame represent a
        # write (aka a segment) and contains one digest per series
        # column + 2*N extra columns that encode start-stop values (N
        # being the number of index columns of the series) + a column
        # containing the series name (like that we can write all the
        # series in one commit)

        if not isinstance(frame, Frame):
            frame = Frame(self.schema, frame)
        # Make sure frame is sorted
        sort_mask = frame.argsort()
        assert (sort_mask == arange(len(sort_mask))).all(), "Dataframe is not sorted!"

        # Save segments
        all_dig = []
        with Pool() as pool:
            for name in self.schema:
                arr = self.schema[name].cast(frame[name])
                data = self.schema[name].codec.encode(arr)
                digest = default_hash(data).digest()
                all_dig.append(digest)
                folder, filename = hashed_path(digest.hex())
                pool.submit(self.pod.cd(folder).write, filename, data)

        # Build commit info
        start = start or frame.start()  # XXX Use numpy.quantile ?
        stop = stop or frame.stop()

        # Combine with last commit
        leaf_rev = self.changelog.leaf()
        if leaf_rev:
            payload = leaf_rev.read()
            leaf_ci = Commit.decode(self.schema, payload)
            new_ci = leaf_ci.update(self.label, start, stop, all_dig, len(frame))
            # TODO early return if new_ci == leaf_ci
        else:
            new_ci = Commit.one(
                self.schema, self.label, start, stop, all_dig, len(frame)
            )

        # Create new digest
        if batch:
            ...  # TODO
            return

        payload = new_ci.encode()
        key = hexdigest(payload)
        rev = self.changelog.commit(
            payload, key=key, parent=leaf_rev and leaf_rev.child
        )
        return rev

    def digests(self):
        for revision in self.revisions():
            yield from revision["digests"]

    def __getitem__(self, by):
        return Query(self)[by]

    def __matmul__(self, by):
        return Query(self) @ by

    def __len__(self):
        # TODO select only index columns
        return len(Query(self, select=list(self.schema.idx)))

    def paginate(self, step=100_000, **kw):
        return Query(self).paginate(step=step, **kw)

    def frame(self, **kw):
        return Query(self, **kw).frame()

    def read(self, **kw):
        return Query(self, **kw).frame()

    def df(self, **kw):
        return Query(self, **kw).df()


class Commit:

    digest_codec = Codec(f"S{binhash_len}", "zstd")
    len_codec = Codec("int")
    label_codec = Codec("str")

    def __init__(self, schema, label, start, stop, digest, length):
        self.schema = schema
        self.label = label  # Array of str
        self.start = start  # Dict of Arrays
        self.stop = stop  # Dict of arrays
        self.digest = digest  # Dict of arrays
        self.length = length  # Array of int

    @classmethod
    def one(cls, schema, label, start, stop, digest, length):
        label = [label]
        start = dict(zip(schema.idx, ([s] for s in start)))
        stop = dict(zip(schema.idx, ([s] for s in stop)))
        digest = dict(zip(schema, ([d] for d in digest)))
        length = [length]
        return Commit(schema, label, start, stop, digest, length)

    @classmethod
    def decode(cls, schema, payload):
        msgpck = registry.codec_registry["msgpack2"]()
        data = msgpck.decode(payload)[0]
        values = {"digest": {}}
        # Decode starts and stops
        for attr in ("start", "stop"):
            attr_vals = {}
            for name in schema.idx:
                attr_vals[name] = schema[name].codec.decode(data[attr][name])
            values[attr] = attr_vals

        # Decode digests
        for name in schema:
            values["digest"][name] = cls.digest_codec.decode(data["digest"][name])

        # Decode len and labels
        values["length"] = cls.len_codec.decode(data["length"])
        values["label"] = cls.label_codec.decode(data["label"])
        return Commit(schema, **values)

    def encode(self):
        msgpck = registry.codec_registry["msgpack2"]()
        data = {"digest": {}}
        # Encode starts and stops
        for attr in ("start", "stop"):
            attr_vals = {}
            for pos, name in enumerate(self.schema.idx):
                arr = getattr(self, attr)[name]
                attr_vals[name] = self.schema[name].codec.encode(arr)
            data[attr] = attr_vals

        # Encode digests
        for name in self.schema:
            data["digest"][name] = self.digest_codec.encode(self.digest[name])

        # Encode len and labels
        data["length"] = self.len_codec.encode(self.length)
        data["label"] = self.label_codec.encode(self.label)
        return msgpck.encode([data])

    def split(self, label, start, stop):
        start_values = {"label": self.label}
        start_values.update(self.start)
        stop_values = {"label": self.label}
        stop_values.update(self.stop)

        frm_start = Frame(Schema.from_frame(start_values), start_values)
        frm_stop = Frame(Schema.from_frame(stop_values), stop_values)
        start_pos = frm_stop.index((label,) + start, right=True)
        stop_pos = frm_start.index((label,) + stop, right=False)
        return start_pos, stop_pos

    @classmethod
    def concat(cls, commit, *other_commits):
        schema = commit.schema
        all_ci = (commit,) + other_commits
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

        return Commit(schema, label, start, stop, digest, length)

    def slice(self, *pos):
        slc = slice(*pos)
        schema = self.schema
        start = {name: self.start[name][slc] for name in schema.idx}
        stop = {name: self.stop[name][slc] for name in schema.idx}
        digest = {name: self.digest[name][slc] for name in schema}
        label = self.label[slc]
        length = self.length[slc]
        return Commit(schema, label, start, stop, digest, length)

    def head(self, pos):
        return self.slice(None, pos)

    def tail(self, pos):
        return self.slice(pos, None)

    def update(self, label, start, stop, digest, length):
        start_pos, stop_pos = self.split(label, start, stop)
        head = self.head(start_pos)
        tail = self.tail(stop_pos)
        # TODO rewrite stop of last row of head and start of first row of tail

        new_ci = Commit.one(self.schema, label, start, stop, digest, length)
        return Commit.concat(head, new_ci, tail)

    def segments(self, label, pod):
        res = []
        for (pos,) in where(self.label == label):
            start = [arr[pos] for arr in self.start.values()]
            stop = [arr[pos] for arr in self.stop.values()]
            digest = [arr[pos] for arr in self.digest.values()]
            length = self.length[pos]
            # convert digests to hex
            digest = [d.hex() for d in digest]
            sgm = ShallowSegment(
                self.schema,
                pod,
                digest,
                start=start,
                stop=stop,
                length=length,
            )
            res.append(sgm)
        return res


class Query:
    def __init__(self, series, **kw):
        self.series = series
        self.params = {
            "closed": "left",
        }
        for k, v in kw.items():
            self.set_param(k, v)

    def set_param(self, key, value):
        if key == "closed":
            if not value in ("left", "right", "both", None):
                raise ValueError(f"Unsupported value {value} for closed")
            self.params["closed"] = value
        elif key in ("start", "stop"):
            self.params[key] = self.series.schema.deserialize(value)
        else:
            if not key in ("limit", "offset", "before", "after", "select"):
                raise ValueError(f"Unsupported parameter: {key}")
            self.params[key] = value

    def __getitem__(self, by):
        if isinstance(by, slice):
            return self @ {"start": by.start, "stop": by.stop}
        elif isinstance(by, (list, tuple, str)):
            return self @ {"select": by}
        else:
            raise KeyError(by)

    def __matmul__(self, kw):
        if not kw:
            return self
        params = self.params.copy()
        params.update(kw)
        return Query(self.series, **params)

    def segments(self):
        keys = ("start", "stop", "before", "after", "closed")
        kw = {k: self.params.get(k) for k in keys}
        segments = self.series.segments(**kw)
        return segments

    def __len__(self):
        return sum(len(s) for s in self.segments())

    def frame(self, **kw):
        qr = self @ kw
        segments = qr.segments()
        limit = qr.params.get("limit")
        offset = qr.params.get("offset")
        select = qr.params.get("select")
        return Frame.from_segments(
            qr.series.schema, segments, limit=limit, offset=offset, select=select
        )

    def df(self, **kw):
        frm = self.frame(**kw)
        return frm.df()

    def paginate(self, step=100_000, **kw):
        if step <= 0:
            raise ValueError("step argument must be > 0")
        qr = self @ kw
        segments = qr.segments()
        select = qr.params.get("select")
        limit = qr.params.get("limit")
        pos = qr.params.get("offset") or 0

        while True:
            lmt = step if limit is None else min(step, limit)
            frm = Frame.from_segments(
                qr.series.schema, segments, limit=lmt, offset=pos, select=select
            )
            if len(frm) == 0:
                return
            if limit is not None:
                limit -= len(frm)
            yield frm
            pos += step


class KVSeries(Series):
    def write(self, frame, start=None, stop=None, root=False, batch=False):
        if root or not (start is None is stop):
            return super().write(frame, start=start, stop=stop, root=root, batch=batch)

        if not isinstance(frame, Frame):
            frame = Frame(self.schema, frame).sorted()

        start = self.schema.row(frame, pos=0, full=False)
        stop = self.schema.row(frame, pos=-1, full=False)
        segments = self.segments(start, stop, closed="both")
        db_frm = Frame.from_segments(
            self.schema, segments
        )  # Maybe paginate on large results

        if db_frm.empty:
            return super().write(frame, batch=batch)

        if db_frm == frame:
            # Nothing to do
            return

        # Concat both frame and reduce it
        new_frm = Frame.concat(frame, db_frm)
        reduce_kw = {c: c for c in self.schema.idx}
        non_idx = [c for c in self.schema if c not in self.schema.idx]
        reduce_kw.update({c: f"(first self.{c})" for c in non_idx})
        new_frm = new_frm.reduce(**reduce_kw)
        return super().write(new_frm, batch=batch)

    def delete(self, *keys):
        # XXX we have 4 delete method (on series, kvseries, collection
        # and repo), we should get rid of some

        # Create a frame with all the existing keys contained
        # between max and min of keys
        if not keys:
            return

        # XXX use changelog pack ?
        start, stop = min(keys), max(keys)
        frm = self[start:stop].frame(closed="both")
        # Keep only keys not given as argument
        # FIXME use frame.mask to filter it
        items = [(k, s) for k, s in zip(frm["label"], frm["meta"]) if k not in keys]
        if len(items) == 0:
            new_frm = self.schema.cast()
        else:
            keep_keys, keep_meta = zip(*items)
            new_frm = {
                "label": keep_keys,
                "meta": keep_meta,
            }
        # Write result to db
        self.write(new_frm, start=start, stop=stop, root=True)
