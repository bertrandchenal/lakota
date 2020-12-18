from time import time

from numpy import arange, issubdtype

from .changelog import phi
from .commit import Commit
from .frame import Frame
from .utils import Interval, Pool, encoder, hashed_path, hexdigest

__all__ = ["Series", "KVSeries"]


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

    def segments(
        self,
        start=None,
        stop=None,
        before=None,
        closed="left",
    ):
        """
        Find matching segments
        """
        leaf_rev = self.changelog.leaf(before=before)
        if not leaf_rev:
            return

        leaf_ci = leaf_rev.commit(self.collection)
        # TODO pass closed to call hereunder
        return leaf_ci.segments(self.label, self.pod, start, stop)

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
        assert issubdtype(schema[head_col].codec.dt, "datetime64")

        revisions = self.changelog.log()
        if not revisions:
            return None
        min_period = min(self.period(rev) for rev in revisions)
        target = min_period * size
        return Interval.bisect(target)

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
        # Each commit is like a frame. A row in this frame represent a
        # write (aka a segment) and contains one digest per series
        # column + 2*N extra columns that encode start-stop values (N
        # being the number of index columns of the series) + a column
        # containing the series name (like that we can write all the
        # series in one commit)

        if not isinstance(frame, Frame):
            frame = Frame(self.schema, self.schema.cast(frame))

        # Make sure frame is sorted
        sort_mask = frame.argsort()
        assert (sort_mask == arange(len(sort_mask))).all(), "Dataframe is not sorted!"

        # Save segments
        all_dig = []
        arr_length = None
        with Pool() as pool:
            for name in self.schema:
                arr = self.schema[name].cast(frame[name])
                if arr_length is None:
                    arr_length = len(arr)
                elif len(arr) != arr_length:
                    raise ValueError("Length mismatch")
                data = self.schema[name].codec.encode(arr)
                digest = hexdigest(data)
                all_dig.append(digest)
                folder, filename = hashed_path(digest)
                pool.submit(self.pod.cd(folder).write, filename, data)

        # Build commit info
        start = start or frame.start()  # XXX Use numpy.quantile ?
        stop = stop or frame.stop()
        if not isinstance(start, tuple):
            start = (start,)
        if not isinstance(stop, tuple):
            stop = (stop,)

        # Create new digest
        if batch:
            ci_info = (self.label, start, stop, all_dig, len(frame))
            batch.append(ci_info)
            return
        self.commit(start, stop, all_dig, len(frame), root=root)

    def commit(self, start, stop, all_dig, length, root=False):
        # root force commit on phi
        leaf_rev = None if root else self.changelog.leaf()

        # Combine with last commit
        if leaf_rev:
            leaf_ci = leaf_rev.commit(self.collection)
            new_ci = leaf_ci.update(self.label, start, stop, all_dig, length)
            # TODO early return if new_ci == leaf_ci
        else:
            new_ci = Commit.one(self.schema, self.label, start, stop, all_dig, length)

        payload = new_ci.encode()
        parent = leaf_rev.child if leaf_rev else phi
        return self.changelog.commit(payload, parents=[parent])

    def __getitem__(self, by):
        return Query(self)[by]

    def __matmul__(self, by):
        return Query(self) @ by

    def __len__(self):
        return len(Query(self, select=list(self.schema.idx)))

    def paginate(self, step=100_000, **kw):
        return Query(self).paginate(step=step, **kw)

    def frame(self, **kw):
        return Query(self, **kw).frame()

    def df(self, **kw):
        return Query(self, **kw).df()


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
            if not key in ("limit", "offset", "before", "select"):
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
        keys = ("start", "stop", "before", "closed")
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
            qr.series.schema,
            segments,
            limit=limit,
            offset=offset,
            select=select,
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
        self.write(new_frm, start=start, stop=stop)
