from time import time

from numpy import arange, lexsort

from .changelog import Changelog, phi
from .frame import Frame
from .utils import hashed_path


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

    def read(self, start=None, end=None, limit=None, after=None, before=None):
        """
        Read all matching frame and combine them
        """
        # Extract start and end
        start = self.schema.deserialize(start)
        end = self.schema.deserialize(end)

        # Collect all revisions
        all_revision = []
        for rev in self.changelog.walk():
            # from baltic import utils
            # if utils.DEBUG:
            #     import pdb;pdb.set_trace()

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
        frames = list(self._read(all_revision, start, end, limit=limit))

        if not frames:
            return Frame(self.schema)

        # Sort (non-overlaping frames)
        frames.sort(key=lambda s: s.start())
        frm = Frame.concat(self.schema, *frames)
        if limit is not None:
            frm = frm.slice(slice(0, limit))
        return frm

    def _read(self, revisions, start, end, limit=None, closed="both"):
        for pos, revision in enumerate(revisions):
            match = intersect(revision, start, end)
            if not match:
                continue
            mstart, mend = match

            # instanciate frame
            frm = Frame.from_pod(
                self.schema,
                self.segment_pod,
                digests=revision["digests"],
                length=revision["len"],
            )

            # Adapt closed value for extremities
            if closed == "right" and mstart != start:
                closed = "both"
            elif closed == "left" and mend != end:
                closed = "both"
            frm = frm.index_slice(mstart, mend, closed=closed)
            if not frm.empty():
                yield frm
                # We have found one result and the search range is
                # collapsed, stop recursion:
                if start and start == end:
                    return
            # recurse left
            if mstart > start:
                left_frm = self._read(
                    revisions[pos + 1 :], start, mstart, limit=limit, closed="left"
                )
                yield from left_frm
            # recurse right
            if not end or mend < end:
                if limit is not None:
                    limit = limit - len(frm)
                    if limit < 1:
                        break
                right_frm = self._read(
                    revisions[pos + 1 :], mend, end, limit=limit, closed="right"
                )
                yield from right_frm

            break

    def write(self, df, start=None, end=None, cast=False, parent_commit=None):
        if cast:
            df = self.schema.cast(df)
        frm = Frame.from_df(self.schema, df)
        # Make sure frame is sorted
        idx_cols = reversed(list(frm.schema.idx))
        sort_mask = lexsort([frm[n] for n in idx_cols])
        assert (sort_mask == arange(len(frm))).all(), "Dataframe is not sorted!"

        col_digests = frm.save(self.segment_pod)
        idx_start = start or frm.start()
        idx_end = end or frm.end()

        rev_info = {
            "start": self.schema.serialize(idx_start),
            "end": self.schema.serialize(idx_end),
            "len": len(frm),
            "digests": col_digests,
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
