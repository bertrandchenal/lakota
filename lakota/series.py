from time import time

from numpy import arange, unique

from .changelog import phi
from .frame import Frame
from .utils import Pool, encoder, hashed_path, hexdigest


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

    def pull(self, remote):
        """
        Pull remote series into self
        """
        self.changelog.pull(remote.changelog)
        for revision in self.revisions():
            for dig in revision["digests"]:
                folder, filename = hashed_path(dig)
                path = folder / filename
                if self.pod.isfile(path):
                    continue
                payload = remote.pod.read(path)
                self.pod.write(path, payload)

    def revisions(self):
        fltr = lambda rev: rev["label"] == self.label
        return self.changelog.walk(fltr)

    def refresh(self):
        self.changelog.refresh()

    def read(
        self,
        start=None,
        stop=None,
        after=None,
        before=None,
        closed="left",
    ):
        """
        Find all matching segments
        """
        # Extract start and stop
        start = self.schema.deserialize(start)
        stop = self.schema.deserialize(stop)

        # Collect all revisions
        all_revision = []
        for rev in self.revisions():
            if after is not None and rev["epoch"] < after:  # closed on left
                continue
            elif before is not None and rev["epoch"] >= before:  # right-opened
                continue

            rev["start"] = self.schema.deserialize(rev["start"])
            rev["stop"] = self.schema.deserialize(rev["stop"])
            if intersect(rev, start, stop):
                all_revision.append(rev)

        # Order revision backward
        all_revision = list(reversed(all_revision))
        # Recursive discovery of matching frames
        segments = list(self._read(all_revision, start, stop, closed=closed))

        # Sort (non-overlaping frames)
        segments.sort(key=lambda s: s.start)
        return segments

    def _read(self, revisions, start, stop, closed="left"):
        for pos, revision in enumerate(revisions):
            match = intersect(revision, start, stop)
            if not match:
                continue
            mstart, mstop = match
            clsd = closed
            if closed == "right" and mstart > start:
                clsd = "both"
            elif closed == None and mstart > start:
                clsd = "left"
            if clsd == "left" and (mstop < stop or not stop):
                clsd = "both"
            elif clsd == None and (mstop < stop or not stop):
                clsd = "right"

            # instanciate frame
            sgm = revision.segment(self).slice(mstart, mstop, clsd)
            yield sgm

            # We have found one result and the search range is
            # collapsed, stop recursion:
            if len(start) and start == stop:
                return

            # recurse left
            if mstart > start:
                if closed == "both":
                    clsd = "left"
                elif closed == "right":
                    clsd = None
                else:
                    clsd = closed
                left_frm = self._read(revisions[pos + 1 :], start, mstart, closed=clsd)
                yield from left_frm
            # recurse right
            if not stop or mstop < stop:
                if closed == "both":
                    clsd = "right"
                elif closed == "left":
                    clsd = None
                else:
                    clsd = closed
                right_frm = self._read(revisions[pos + 1 :], mstop, stop, closed=clsd)
                yield from right_frm
            break

    def write(self, frame, start=None, stop=None, root=False, batch=False):
        if not isinstance(frame, Frame):
            frame = Frame(self.schema, frame)
        # Make sure frame is sorted, lexsort gives more weight to the
        # left-most array
        sort_mask = frame.lexsort()
        assert (sort_mask == arange(len(sort_mask))).all(), "Dataframe is not sorted!"

        # Save segments
        all_dig = []
        with Pool() as pool:
            for name in self.schema:
                arr = self.schema[name].cast(frame[name])
                # digest = hexdigest(arr.tobytes())
                data = self.schema[name].encode(arr)
                digest = hexdigest(data)
                all_dig.append(digest)
                folder, filename = hashed_path(digest)
                pool.submit(self.pod.cd(folder).write, filename, data)

        # Build commit info
        start = start or frame.start()
        stop = stop or frame.stop()
        sstart = self.schema.serialize(start)
        sstop = self.schema.serialize(stop)
        # XXX rev_info = {self.label: {}} (to be able to write on several labels)
        rev_info = {
            "start": sstart,
            "stop": sstop,
            "len": len(frame),
            "digests": all_dig,
            "epoch": time(),
            "label": self.label,
        }
        key = hexdigest(
            *encoder(self.label, str(len(frame)), *all_dig, *sstart, *sstop)
        )
        force_parent = phi if root else None

        if batch:
            return rev_info, key
        commit = self.changelog.commit(rev_info, key=key, force_parent=force_parent)
        return commit

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

    def df(self, **kw):
        return Query(self, **kw).df()


class Query:
    def __init__(self, series, **kw):
        self.series = series
        self.segments = None
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

    def read(self):
        keys = ("start", "stop", "before", "after", "closed")
        kw = {k: self.params.get(k) for k in keys}
        segments = self.series.read(**kw)
        return segments

    def __len__(self):
        return sum(len(s) for s in self.read())

    def frame(self, **kw):
        qr = self @ kw
        segments = qr.read()
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
        segments = qr.read()
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
        segments = self.read(start, stop, closed="both")
        db_frm = Frame.from_segments(
            self.schema, segments
        )  # Maybe paginate on large results

        if db_frm.empty:
            return super().write(frame, batch=batch)

        # Concat both frame and reduce it
        new_frm = Frame.concat(frame, db_frm)
        reduce_kw = {c: c for c in self.schema.idx}
        non_idx = [c for c in self.schema if c not in self.schema.idx]
        reduce_kw.update({c: f"(first self.{c})" for c in non_idx})
        new_frm = new_frm.reduce(**reduce_kw)
        return super().write(new_frm, batch=batch)

    def delete(self, *labels):
        # Create a frame with all the existing labels contained
        # between max and min of labels
        if not labels:
            return

        # XXX use changelog pack ?
        start, stop = min(labels), max(labels)
        frm = self[start:stop].frame(closed="both")
        # Keep only labels not given as argument
        items = [(l, s) for l, s in zip(frm["label"], frm["meta"]) if l not in labels]
        if len(items) == 0:
            new_frm = self.schema.cast()
        else:
            keep_labels, keep_meta = zip(*items)
            new_frm = {
                "label": keep_labels,
                "meta": keep_meta,
            }
        # Write result to db
        self.write(new_frm, start=start, stop=stop, root=True)
