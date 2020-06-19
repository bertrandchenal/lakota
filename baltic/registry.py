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
        self.schema_series = Series(self.schema, self.pod / "registry")
        self.series_root = self.pod / "series"

    def clear(self):
        self.pod.clear()

    def create(self, schema, *labels):
        # FIXME prevent double create (here or in the segment)
        sgm = Segment.from_df(
            self.schema, {"label": labels, "schema": [schema.dumps()] * len(labels)}
        )
        self.schema_series.write(sgm)  # SQUASH ?

    def get(self, label):
        sgm = self.schema_series.read()  # TODO use filters!
        idx = sgm.index(label)
        assert sgm["label"][idx] == label
        schema = Schema.loads(sgm["schema"][idx])
        digest = hexdigest(label.encode())
        prefix, suffix = digest[:2], digest[2:]
        series = Series(schema, self.pod / prefix / suffix)
        return series
