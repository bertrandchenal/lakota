from numpy import ascontiguousarray, dtype, issubdtype

from .batch import Batch
from .changelog import phi
from .commit import Commit
from .frame import Frame
from .utils import Closed, Interval, Pool, hashed_path, settings

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
        closed="LEFT",
        from_ci=None,
    ):
        """
        Find matching segments
        """

        if not from_ci:
            # Find leaf commit
            leaf_rev = self.changelog.leaf(before=before)
            if not leaf_rev:
                return
            from_ci = leaf_rev.commit(self.collection)
        return from_ci.segments(self.label, self.pod, start, stop, closed=closed)

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

    def write(self, frame, start=None, stop=None, closed="b", root=False):
        # Each commit is like a frame. A row in this frame represent a
        # write (aka a segment) and contains one digest per series
        # column + 2*N extra columns that encode start-stop values (N
        # being the number of index columns of the series) + a column
        # containing the series name (like that we can write all the
        # series in one commit)

        frame = Frame(self.schema, frame)

        # Make sure frame is sorted
        # XXX forbid repeated values in index ??
        assert frame.is_sorted(), "Frame is not sorted!"

        # Save segments
        all_dig = []
        arr_length = None
        embedded = {}
        with Pool() as pool:
            for name in self.schema:
                # Cast array & check len
                values = frame[name]
                if arr_length is None:
                    arr_length = len(values)
                elif len(values) != arr_length:
                    raise ValueError("Length mismatch")
                digest, embed_data = self._write_col(name, values, pool)
                all_dig.append(digest)
                if embed_data is not None:
                    embedded[digest] = embed_data

        # Build commit info
        start = frame.start() if start is None else start
        stop = frame.stop() if stop is None else stop
        if not isinstance(start, tuple):
            start = (start,)
        if not isinstance(stop, tuple):
            stop = (stop,)

        # Create new digest
        batch = self.collection.batch
        if batch:
            ci_info = (self.label, start, stop, all_dig, len(frame), closed, embedded)
            if isinstance(batch, Batch):
                batch.append(*ci_info)
            else:
                return ci_info
            return
        return self.commit(
            start,
            stop,
            all_dig,
            len(frame),
            root=root,
            closed=closed,
            embedded=embedded,
        )

    def _write_col(self, name, values, pool):
        # Encode content
        arr = self.schema[name].cast(values)
        # Create digest (based on actual array for simple
        # type, based on encoded content for O and U)
        codec = self.schema[name].codec
        data, digest = codec.encode(arr, with_digest=True)

        embedded_data = None
        if (
            len(data) < settings.embed_max_size
        ):  # every small array gets embedded
            # Put small arrays aside
            embedded_data = data
        else:
            folder, filename = hashed_path(digest)
            # XXX move writing in Series.commit and handle situation where the commit gets to large?
            # XXX keep it here for when a batch gets too large ?
            pool.submit(self.pod.cd(folder).write, filename, data)
        return digest, embedded_data

    def update(self, frame):
        frame = Frame(self.schema, frame)
        start, stop = frame.start(), frame.stop()
        idx = tuple(self.schema.idx)
        upd_cols = tuple(c for c in frame if c not in idx)
        read_cols = tuple(c for c in self.schema.columns if c not in idx + upd_cols)
        db_frm = self.frame(start=start, stop=stop, closed="b", select=idx + read_cols)
        db_start, db_stop = db_frm.start(), db_frm.stop()
        overlap_frm = frame.islice(db_start, db_stop, "b")
        head_frm = frame.islice(None, db_start, "l")
        tail_frm = frame.islice(db_stop, None, "r")

        # Make sure index matches on overlapping part
        for col in idx:
            if (
                len(db_frm) != len(overlap_frm)
                or (db_frm[col] != overlap_frm[col]).any()
            ):
                raise ValueError("Update frame is not aligned with existing index")

        # Update columns
        for col in upd_cols:
            db_frm[col] = overlap_frm[col]

        # Add columns filled with zero-like values in non-overlapping
        # frames
        for frm in (head_frm, tail_frm):
            for col in read_cols:
                frm[col] = self.schema[col].zeroes(len(frm))

        full_frm = Frame.concat(head_frm, db_frm, tail_frm)
        return self.write(full_frm, start, stop, closed="b")

    def commit(
        self, start, stop, all_dig, length, root=False, closed="b", embedded=None
    ):
        # root force commit on phi
        leaf_rev = None if root else self.changelog.leaf()

        # Combine with last commit
        if leaf_rev:
            leaf_ci = leaf_rev.commit(self.collection)
            new_ci = leaf_ci.update(
                self.label,
                start,
                stop,
                all_dig,
                length,
                closed=closed,
                embedded=embedded,
            )
            # TODO early return if new_ci == leaf_ci
        else:
            new_ci = Commit.one(
                self.schema,
                self.label,
                start,
                stop,
                all_dig,
                length,
                closed=closed,
                embedded=embedded,
            )

        payload = new_ci.encode()
        parent = leaf_rev.child if leaf_rev else phi
        return self.changelog.commit(payload, parents=[parent])

    def delete(self, start, stop, closed="b", root=False):
        frm = {k:[] for k in self.schema}
        return self.write(frame=frm, start=start, stop=stop, closed=closed,
                          root=root)

    def __getitem__(self, by):
        return Query(self)[by]

    def __matmul__(self, by):
        return Query(self) @ by

    def __len__(self):
        return len(Query(self, select=list(self.schema.idx)))

    def paginate(self, step=settings.page_len, **kw):
        return Query(self).paginate(step=step, **kw)

    def frame(self, **kw):
        return Query(self, **kw).frame()

    def df(self, **kw):
        # TODO implement order (to "tail" a series)! (or .start and .stop based on last rev)
        return Query(self, **kw).df()

class Query:
    def __init__(self, series, **kw):
        self.series = series
        self.params = {
            "closed": "LEFT",
        }
        for k, v in kw.items():
            self.set_param(k, v)

    def set_param(self, key, value):
        if key == "closed":
            self.params["closed"] = Closed.cast(value)
        elif key in ("start", "stop"):
            self.params[key] = self.series.schema.deserialize(value)
        else:
            if not key in ("limit", "offset", "before", "select", "from_ci"):
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
        keys = ("start", "stop", "before", "closed", "from_ci")
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

    def paginate(self, step=settings.page_len, **kw):
        if step <= 0:
            raise ValueError("step argument must be > 0")
        qr = self @ kw
        segments = qr.segments()
        select = qr.params.get("select")
        limit = qr.params.get("limit")
        pos = qr.params.get("offset") or 0
        while True:
            lmt = step if limit is None else min(step, limit)
            offset = pos
            frm = Frame.from_segments(
                qr.series.schema, segments, limit=lmt, offset=offset, select=select
            )
            if len(frm) == 0:
                return
            if limit is not None:
                limit -= len(frm)
            yield frm
            pos += step


class KVSeries(Series):
    def write(self, frame, start=None, stop=None, closed="b", root=False):
        if root or not (start is None is stop):
            return super().write(frame, start=start, stop=stop, root=root)

        if not isinstance(frame, Frame):
            frame = Frame(self.schema, frame).sorted()

        segments = self.segments(frame.start(), frame.stop(), closed="BOTH")
        db_frm = Frame.from_segments(
            self.schema, segments
        )  # Maybe paginate on large results

        if db_frm.empty:
            return super().write(frame, closed=closed)

        if db_frm == frame:
            # Nothing to do
            return

        # Concat both frame and reduce it
        new_frm = Frame.concat(frame, db_frm)
        reduce_kw = {c: c for c in self.schema.idx}
        non_idx = [c for c in self.schema if c not in self.schema.idx]
        reduce_kw.update({c: f"(first self.{c})" for c in non_idx})
        new_frm = new_frm.reduce(**reduce_kw)
        return super().write(new_frm)  # XXX pass closed ?

    def delete(self, *keys):
        # XXX we have 4 delete method (on series, kvseries, collection
        # and repo), we should get rid of some

        # Create a frame with all the existing keys contained
        # between max and min of keys
        if not keys:
            return

        # XXX use changelog pack ?
        start, stop = min(keys), max(keys)
        frm = self[start:stop].frame(closed="BOTH")
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
        revs = self.write(new_frm, start=start, stop=stop)
        return revs

    # def delete(self, *keys):
    #     if not keys:
    #         return
    #     frm = self.frame()
    #     mask = '(logical_not (isin self.label {}))'.format(
    #         ' '.join(f'"{k}"' for k in keys)
    #     )
    #     new_frm = frm.mask(mask)
    #     self.write(new_frm, start=frm.start(), stop=frm.stop())
