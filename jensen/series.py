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
        self,
        start=None,
        stop=None,
        head=None,
        tail=None,
        after=None,
        before=None,
        closed="both",
    ):
        """
        Read all matching frame and combine them
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
        frm = Frame.from_segments(self.schema, *segments, head=head, tail=tail)

        return frm

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
        frm = self.read()
        commit = self.write(frm, parent_commit=phi)
        self.truncate(commit.path)

    def digests(self):
        for revision in self.changelog.walk():
            yield from revision["digests"]
