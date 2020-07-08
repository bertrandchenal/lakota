from numpy import arange, lexsort

from .changelog import Changelog, phi
from .segment import Segment


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

    def __init__(self, schema, pod, segment_pod=None):
        self.schema = schema
        self.pod = pod
        self.segment_pod = segment_pod or pod / "segment"
        self.chl_pod = self.pod / "changelog"
        self.changelog = Changelog(self.chl_pod)

    def clone(self, remote, shallow=False):
        """
        Clone remote series into self
        """
        self.changelog.pull(remote.changelog)
        for revision in self.changelog.walk():
            for dig in revision["columns"]:
                prefix, suffix = dig[:2], dig[2:]
                path = f"{prefix}/{suffix}"
                payload = remote.segment_pod.read(path)
                self.segment_pod.write(path, payload)

    def read(self, start=None, end=None, limit=None):
        """
        Read all matching segment and combine them
        """
        start = self.schema.deserialize(start)
        end = self.schema.deserialize(end)

        # Collect all rev revision
        all_revision = []
        for revision in self.changelog.walk():
            revision["start"] = self.schema.deserialize(revision["start"])
            revision["end"] = self.schema.deserialize(revision["end"])
            if intersect(revision, start, end):
                all_revision.append(revision)

        # Order revision backward
        all_revision = list(reversed(all_revision))
        # Recursive discovery of matching segments
        segments = self._read(all_revision, start, end, limit=limit)

        if not segments:
            return Segment(self.schema)

        # Sort (non-overlaping segments)
        segments.sort(key=lambda s: s.start())
        return Segment.concat(self.schema, *segments)

    def _read(self, revisions, start, end, limit=None):
        segments = []
        for pos, revision in enumerate(revisions):
            match = intersect(revision, start, end)
            if not match:
                continue

            mstart, mend = match
            # instanciate segment
            sgm = Segment.from_pod(self.schema, self.segment_pod, revision["columns"])
            sgm = sgm.slice(mstart, mend, closed="both")
            segments.append(sgm)

            # We have found one result and the search range is
            # collapsed:
            if start and start == end:
                return segments
            # recurse left
            if mstart > start:
                left_sgm = self._read(revisions[pos + 1 :], start, mstart, limit=limit)
                segments = left_sgm + segments
            # recurse right
            if not end or mend < end:
                if limit is not None:
                    limit = limit - len(sgm)
                    if limit < 1:
                        break
                right_sgm = self._read(revisions[pos + 1 :], mend, end, limit=limit)
                segments = segments + right_sgm

            break
        return segments

    def write(self, df, start=None, end=None, cast=False, parent_commit=None):
        if cast:
            df = self.schema.cast(df)

        sgm = Segment.from_df(self.schema, df)
        # Make sure segment is sorted
        sort_mask = lexsort([sgm[n] for n in reversed(sgm.schema.idx)])
        assert (sort_mask == arange(len(sgm))).all()

        col_digests = sgm.save(self.segment_pod)
        idx_start = start or sgm.start()
        idx_end = end or sgm.end()

        revision = {
            "start": self.schema.serialize(idx_start),
            "end": self.schema.serialize(idx_end),
            "size": sgm.size(),
            "columns": col_digests,
        }
        return self.changelog.commit(revision, parent=parent_commit)

    def truncate(self, skip=None):
        self.chl_pod.clear(skip=skip)

    def squash(self):
        """
        Remove all the revisions, collapse all segments into one
        """
        sgm = self.read()
        key = self.write(sgm, parent_commit=phi)
        self.truncate(skip=[key])

    def digests(self):
        for revision in self.changelog.walk():
            yield from revision["columns"]
