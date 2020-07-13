from itertools import chain
from time import time

from .pod import POD
from .schema import Schema
from .segment import Segment
from .series import Series
from .utils import hexdigest, hashed_path

# Idea: "package" a bunch of writes in a Zip/Tar and send the
# archive on s3


class Registry:
    """
    Use a Series object to store all the series labels
    """

    schema = Schema(["label:str", "timestamp:f8", "schema:O"])

    # TODO key should become the "keyspace" aka the registry generation that should be created upront


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
            # Create a segment of size one
            sgm = Segment.from_df(
                self.schema,
                {"label": [label], "timestamp": [time()], "schema": [schema.as_dict()]},
            )
            self.schema_series.write(sgm)

    def search(self, label=None):
        if label:
            start = end = (label,)
        else:
            start = end = None
        sgm = self.schema_series.read(start=start, end=end)
        return sgm

    def get(self, label, from_sgm=None):
        if from_sgm:
            sgm = from_sgm.slice([label])
        else:
            sgm = self.search(label)
        assert not sgm.empty(), f"Label '{label}' not found"
        schema = Schema.loads(sgm["schema"][-1])
        timestamp = sgm["timestamp"][-1]
        digest = hexdigest(label.encode(), str(timestamp).encode())
        folder, filename = hashed_path(digest)
        series = Series(schema, self.series_pod / folder / filename, self.segment_pod)
        return series

    def gc(self, soft=True):
        """
        Loop on all series, collect all used digests, and delete obsolete
        ones. If soft if true, obsolete revision are moved to an
        archive location. If soft is false, obsolete revisions are deleted.
        """
        sgm = self.search()
        labels = set(sgm["label"])
        series = (self.get(l, sgm) for l in labels)
        per_series = (s.digests() for s in series)
        active_digests = set(chain.from_iterable(per_series))
        active_digests.update(self.schema_series.digests())
        count = 0
        for filename in self.segment_pod.walk():
            digest = filename.replace('/', '')
            if digest not in active_digests:
                count += 1
                self.segment_pod.rm(filename)

        return count
