from time import time

from numpy import arange, lexsort

from .changelog import Changelog, phi
from .frame import Frame, ShallowSegment
from .utils import hashed_path, hexdigest


def intersect(revision, start, end):
    ok_start = not end or revision["start"][: len(end)] <= end
    ok_end = not start or revision["end"][: len(start)] >= start
    if not (ok_start and ok_end):
        return None
    # return reduced range
    max_start = max(revision["start"], start)
    min_end = min(revision["end"], end) if end else revision["end"]
    return (max_start, min_end)


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

    def clone(self, remote, shallow=False):
        """
        Clone remote series into self
        """
        # TODO implement push & pull
        self.changelog.pull(remote.changelog)
        # if shallow:
        #     return
        for revision in self.changelog.walk():
            for dig in revision["digests"]:
                folder, filename = hashed_path(dig)
                path = folder / filename
                payload = remote.segment_pod.read(path)
                self.segment_pod.write(path, payload)

    def revisions(self):
        return self.changelog.walk()

    # TODO replace end with stop everywhere!
    def read(
        self, start=None, end=None, limit=None, after=None, before=None, closed="both"
    ):
        """
        Read all matching frame and combine them
        """
        # Extract start and end
        start = self.schema.deserialize(start)
        end = self.schema.deserialize(end)

        # Collect all revisions
        all_revision = []
        for rev in self.changelog.walk():
            if after is not None and rev["epoch"] < after:  # closed on left
                continue
            elif before is not None and rev["epoch"] >= before:  # right-opened
                continue

            rev["start"] = self.schema.deserialize(rev["start"])
            rev["end"] = self.schema.deserialize(rev["end"])
            if intersect(rev, start, end):
                all_revision.append(rev)

        # Order revision backward
        all_revision = list(reversed(all_revision))
        # Recursive discovery of matching frames
        segments = list(self._read(all_revision, start, end, closed=closed))

        # Sort (non-overlaping frames)
        segments.sort(key=lambda s: s.start)
        frm = Frame.from_segments(self.schema, *segments, limit=limit)

        return frm

    def _read(self, revisions, start, end, closed="both"):
        for pos, revision in enumerate(revisions):
            match = intersect(revision, start, end)
            if not match:
                continue
            mstart, mend = match
            if closed == "right" and mstart > start:
                closed = "both"
            if closed == "left" and mend < end:
                closed = "both"

            # instanciate frame
            sgm = ShallowSegment(
                self.schema,
                self.segment_pod,
                revision["digests"],
                start=revision["start"],
                stop=revision["end"],
                length=revision["len"],
            ).slice(mstart, mend, closed)
            yield sgm

            # We have found one result and the search range is
            # collapsed, stop recursion:
            if len(start) and start == end:
                return

            # Adapt closed value for extremities
            if closed == "right" and mstart != start:
                closed = "both"
            elif closed == "left" and mend != end:
                closed = "both"

            # recurse left
            if mstart > start:
                closed = "left"  # "both" if start < mstart else "left"
                left_frm = self._read(
                    revisions[pos + 1 :], start, mstart, closed=closed
                )
                yield from left_frm
            # recurse right
            if not end or mend < end:
                right_frm = self._read(revisions[pos + 1 :], mend, end, closed="right")
                yield from right_frm
            break

    def write(self, frame, start=None, end=None, parent_commit=None):
        if not isinstance(frame, Frame):
            frame = Frame(self.schema, frame)
        # Make sure frame is sorted
        idx_cols = reversed(list(self.schema.idx))
        sort_mask = lexsort([frame[n] for n in idx_cols])
        assert (sort_mask == arange(len(sort_mask))).all(), "Dataframe is not sorted!"

        # Save segments (XXX autochunkify)
        all_dig = []
        for name in self.schema:
            arr = self.schema[name].cast(frame[name])
            digest = hexdigest(arr.tostring())
            all_dig.append(digest)
            data = self.schema[name].encode(arr)
            folder, filename = hashed_path(digest)
            self.segment_pod.cd(folder).write(filename, data)

        start = start or self.schema.row(frame, pos=0, full=False)
        end = end or self.schema.row(frame, pos=-1, full=False)
        rev_info = {
            "start": self.schema.serialize(start),
            "end": self.schema.serialize(end),
            "len": len(frame),
            "digests": all_dig,
            "epoch": time(),
        }
        commit = self.changelog.commit(rev_info, force_parent=parent_commit)
        return commit

    def truncate(self, *skip):
        self.chl_pod.clear(*skip)

    def squash(self):
        """
        Remove all the revisions, collapse all frames into one
        """
        frm = self.read()
        commit = self.write(frm, parent_commit=phi)
        self.truncate(commit.path)

    def digests(self):
        for revision in self.changelog.walk():
            yield from revision["digests"]
