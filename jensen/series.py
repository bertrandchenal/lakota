from time import time

from numpy import arange, lexsort

from .changelog import Changelog, phi
from .frame import Frame
from .utils import hashed_path, hexdigest


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

    def __init__(self, label, schema, pod, segment_pod=None):
        self.schema = schema
        self.pod = pod
        self.segment_pod = segment_pod or pod / "segment"
        self.chl_pod = self.pod / "changelog"
        self.changelog = Changelog(self.chl_pod)
        self.label = label

    def pull(self, remote):
        """
        Pull remote series into self
        """
        self.changelog.pull(remote.changelog)
        for revision in self.changelog.walk():
            for dig in revision["digests"]:
                folder, filename = hashed_path(dig)
                path = folder / filename
                if self.segment_pod.isfile(path):
                    continue
                payload = remote.segment_pod.read(path)
                self.segment_pod.write(path, payload)

    def revisions(self):
        return self.changelog.walk()

    def read(
        self, start=None, stop=None, after=None, before=None, closed="both",
    ):
        """
        Find all matching segments
        """
        # Extract start and stop
        start = self.schema.deserialize(start)
        stop = self.schema.deserialize(stop)

        # Collect all revisions
        all_revision = []
        for rev in self.changelog.walk():
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

    def _read(self, revisions, start, stop, closed="both"):
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

    def write(self, frame, start=None, stop=None, parent_commit=None):
        if not isinstance(frame, Frame):
            frame = Frame(self.schema, frame)
        # Make sure frame is sorted
        idx_cols = reversed(list(self.schema.idx))
        sort_mask = lexsort([frame[n] for n in idx_cols])
        assert (sort_mask == arange(len(sort_mask))).all(), "Dataframe is not sorted!"

        # Save segments (TODO auto-chunk)
        all_dig = []
        for name in self.schema:
            arr = self.schema[name].cast(frame[name])
            digest = hexdigest(arr.tobytes())
            all_dig.append(digest)
            data = self.schema[name].encode(arr)
            folder, filename = hashed_path(digest)
            self.segment_pod.cd(folder).write(filename, data)

        start = start or self.schema.row(frame, pos=0, full=False)
        stop = stop or self.schema.row(frame, pos=-1, full=False)
        rev_info = {
            "start": self.schema.serialize(start),
            "stop": self.schema.serialize(stop),
            "len": len(frame),
            "digests": all_dig,
            "epoch": time(),
        }
        commit = self.changelog.commit(rev_info, force_parent=parent_commit)
        return commit

    def truncate(self, *skip):
        self.chl_pod.clear(*skip)

    def squash(self, expected=None):
        """
        Remove all the revisions, collapse all frames into one
        """
        step = 500_000
        commits = [self.write(frm, parent_commit=phi) for frm in self.paginate(step)]
        self.truncate(*(c.path for c in commits))

    def digests(self):
        for revision in self.changelog.walk():
            yield from revision["digests"]

    def __getitem__(self, by):
        if isinstance(by, slice):
            return Cursor(self)[by]
        else:
            raise KeyError(by)

    @classmethod
    def inject(cls, name):
        fn = lambda self, *a, **kw: getattr(Cursor(self), name)(*a, **kw)
        setattr(cls, name, fn)


class Cursor:
    def __init__(self, series):
        self.series = series
        self.segments = None
        self.params = {
            "closed": "both",  # FIXME should be 'left'
        }

    def closed(self, val):
        assert val in ("left", "right", None), f"Unsupported value {val} for closed"
        self.params["closed"] = val
        return self

    def start(self, val):
        self.params["start"] = self.series.schema.deserialize(val)
        return self

    def stop(self, val):
        self.params["stop"] = self.series.schema.deserialize(val)
        return self

    def limit(self, val):
        self.params["limit"] = val
        return self

    def offset(self, val):
        self.params["offset"] = val
        return self

    def before(self, val):
        self.params["before"] = val
        return self

    def after(self, val):
        self.params["after"] = val
        return self

    def select(self, *columns):
        self.params["select"] = columns
        return self

    def __getitem__(self, by):
        if isinstance(by, slice):
            self.start(by.start).stop(by.stop)
            return self.frame()
        elif isinstance(by, (list, tuple, str)):
            return self.select(by)
        else:
            raise KeyError(by)

    def read(self):
        keys = ("start", "stop", "before", "after", "closed")
        kw = {k: self.params.get(k) for k in keys}
        segments = self.series.read(**kw)
        return segments

    def __len__(self):
        return sum(len(s) for s in self.read())

    def frame(self):
        segments = self.read()
        limit = self.params.get("limit")
        offset = self.params.get("offset")
        select = self.params.get("select")
        return Frame.from_segments(
            self.series.schema, segments, limit=limit, offset=offset, select=select
        )

    def df(self):
        frm = self.frame()
        return frm.df()

    def paginate(self, step=100_000):
        if step <= 0:
            raise ValueError("step argument must be > 0")
        segments = self.read()
        select = self.params.get("select")
        limit = self.params.get("limit")
        pos = self.params.get("offset") or 0

        while True:
            lmt = step if limit is None else min(step, limit)
            frm = Frame.from_segments(
                self.series.schema, segments, limit=lmt, offset=pos, select=select
            )
            if len(frm) == 0:
                return
            if limit is not None:
                limit -= len(frm)
            yield frm
            pos += step


# Auto-magically inject Cursor method in Series
to_inject = [
    "paginate",
    "df",
    "frame",
    "closed",
    "start",
    "stop",
    "limit",
    "offset",
    "after",
    "before",
    "__len__",
    "select",
]
for name in to_inject:
    Series.inject(name)
