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

    schema = Schema(["label:str", "schema:str"])

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
        current = set(self.ls())
        assert not current.intersection(labels)
        sgm = Segment.from_df(
            self.schema, {"label": labels, "schema": [schema.dumps()] * len(labels)}
        )
        self.schema_series.write(sgm)  # SQUASH ?

    def ls(self):
        sgm = self.schema_series.read()  # TODO use filters!
        return sgm["label"]

    def get(self, label):
        sgm = self.schema_series.read()  # TODO use filters!
        idx = sgm.index(label)
        assert sgm["label"][idx] == label
        schema = Schema.loads(sgm["schema"][idx])
        digest = hexdigest(label.encode())
        prefix, suffix = digest[:2], digest[2:]
        series = Series(schema, self.series_pod / prefix / suffix, self.segment_pod)
        return series
