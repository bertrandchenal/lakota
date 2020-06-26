import json
import time

from numpy import arange, lexsort

from .changelog import Changelog
from .segment import Segment


def intersect(info, start, end):
    ok_start = not end or info["start"] <= end
    ok_end = not start or info["end"] >= start
    if not (ok_start and ok_end):
        return None
    # return reduced range
    return (max(info["start"], start), min(info["end"], end))


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
        self.changelog.pull(remote.changelog)
        for content in self.changelog.extract():
            info = json.loads(content)
            for dig in info["columns"]:
                prefix, suffix = dig[:2], dig[2:]
                path = f"{prefix}/{suffix}"
                payload = remote.segment_pod.read(path)
                # TODO skip already existing segments!
                self.segment_pod.write(path, payload)

    def read(self, start=[], end=[], limit=None):
        """
        Read all matching segment and combine them
        """
        start = self.schema.deserialize(start)
        end = self.schema.deserialize(end)

        # Collect all rev info
        series_info = []
        for content in self.changelog.extract():
            info = json.loads(content)
            info["start"] = self.schema.deserialize(info["start"])
            info["end"] = self.schema.deserialize(info["end"])
            if intersect(info, start, end):
                series_info.append(info)
        # Order revision backward
        series_info = list(reversed(series_info))
        # Recursive discovery of matching segments
        segments = self._read(series_info, start, end, limit=limit)

        if not segments:
            return Segment(self.schema)
        return Segment.concat(self.schema, *segments)

    def _read(self, series_info, start, end, limit=None):
        segments = []
        for pos, info in enumerate(series_info):
            match = intersect(info, start, end)
            if not match:
                continue

            # instanciate segment
            sgm = Segment.from_pod(self.schema, self.segment_pod, info["columns"])
            segments.append(sgm.slice(*match, closed="both"))

            mstart, mend = match
            # recurse left
            if mstart > start:
                left_sgm = self._read(
                    series_info[pos + 1 :], start, mstart, limit=limit
                )
                segments = left_sgm + segments

            # recurse right
            if mend < end:
                if limit is not None:
                    limit = limit - len(sgm)
                    if limit < 1:
                        break
                right_sgm = self._read(series_info[pos + 1 :], mend, end, limit=limit)
                segments = segments + right_sgm

            break
        return segments

    def write(self, df, start=None, end=None, cast=False):
        if cast:
            df = self.schema.cast(df)

        sgm = Segment.from_df(self.schema, df)
        # Make sure segment is sorted
        sort_mask = lexsort([sgm[n] for n in sgm.schema.idx])
        assert (sort_mask == arange(len(sgm))).all()

        col_digests = sgm.save(self.segment_pod)
        idx_start = start or sgm.start()
        idx_end = end or sgm.end()

        info = {
            "start": self.schema.serialize(idx_start),
            "end": self.schema.serialize(idx_end),
            "size": sgm.size(),
            "timestamp": time.time(),
            "columns": col_digests,
        }
        content = json.dumps(info)
        self.changelog.commit([content])

    def truncate(self):
        self.chl_pod.clear()

    def squash(self):
        """
        Remove all the revisions, collapse all segments into one
        """

        # FIXME: it would make more sense to create a snapshot and
        # keep historical content in an archive group. (and have
        # another command that remove archives)
        sgm = self.read()
        self.truncate()
        self.write(sgm)

    def digests(self):
        for content in self.changelog.extract():
            info = json.loads(content)
            yield from info["columns"]
