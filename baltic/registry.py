from itertools import chain
from time import time
from uuid import uuid4

from .pod import POD
from .schema import Schema
from .segment import Segment
from .series import Series
from .utils import hexdigest

# Idea: "package" a bunch of writes in a Zip/Tar and send the
# archive on s3


class Registry:
    """
    Use a Series object to store all the series labels
    """

    schema = Schema(["label:str", "key:str", "schema:O"])

    def __init__(self, uri=None, pod=None):
        self.pod = pod or POD.from_uri(uri)
        self.segment_pod = self.pod / "segment"
        self.schema_series = Series(
            self.schema, self.pod / "registry", self.segment_pod
        )
        self.series_pod = self.pod / "series"

    def clear(self):
        self.pod.clear()

    def clone(self, remote, label, shallow=False):
        # XXX define remote in init and simply do registry.clone(label) ?
        assert not shallow, "Shallow clone not supported yet"
        rseries = remote.get(label)
        self.create(rseries.schema, label)
        series = self.get(label)
        series.clone(rseries, shallow=shallow)

    def create(self, schema, *labels):
        for label in sorted(labels):
            current = self.search(label)
            assert current.empty
            key = ".".join(
                (
                    hex(int(time() * 1000))[2:],  # time-based prefix
                    uuid4().hex,  # random suffix to avoid overwriting concurrent creation
                )
            )
            # Create a segment of size one
            sgm = Segment.from_df(
                self.schema,
                {"label": [label], "key": [key], "schema": [schema.as_dict()]},
            )
            self.schema_series.write(sgm)

    def search(self, label=None):
        if label:
            start = end = (label,)
        else:
            start = end = None
        sgm = self.schema_series.read(start=start, end=end)
        return sgm

    def get(self, label):
        sgm = self.search(label)
        assert not sgm.empty()
        schema = Schema.loads(sgm["schema"][0])
        digest = hexdigest(label.encode())
        prefix, suffix = digest[:2], digest[2:]
        series = Series(schema, self.series_pod / prefix / suffix, self.segment_pod)
        return series

    def gc(self, soft=True):
        """
        Loop on all series, collect all used digests, and delete obsolete
        ones. If soft if true, obsolete revision are moved to an
        archive location. If soft is false, obsolete revisions are deleted.
        """
        labels = set(self.search()["label"])
        # TODO repeated calls to self.get method are inefficient
        series = (self.get(l) for l in labels)
        per_series = (s.digests() for s in series)
        active_digests = set(chain.from_iterable(per_series))
        active_digests.update(self.schema_series.digests())
        count = 0
        for folder in self.segment_pod.ls():
            for filename in self.segment_pod.ls(folder):
                digest = folder + filename
                if digest not in active_digests:
                    count += 1
                    self.segment_pod.rm(f"{folder}/{filename}")

        return count
