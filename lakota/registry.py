import re
from itertools import chain

from .pod import POD
from .schema import Schema
from .series import KVSeries, Series
from .utils import hashed_path, hexdigest, logger

LABEL_RE = re.compile("^[a-zA-Z0-9-_\.]+$")


class Registry:
    """
    Use a Series object to store all the series labels
    """

    schema = Schema(["label str*", "schema O"])

    def __init__(self, uri=None, pod=None):
        # TODO add a repo and move all this pod setup in it
        self.pod = pod or POD.from_uri(uri)
        self.segment_pod = self.pod / "segment"
        self.label_series = KVSeries(  # TODO rename into "series"
            "__label_series__", self.schema, self.pod / "registry", self.segment_pod
        )
        self.series_pod = self.pod / "series"

    def pull(self, remote, *labels):
        # Pull schema
        self.label_series.pull(remote.label_series)
        # Extract frames
        local_cache = self.search()
        remote_cache = remote.search()

        if not labels:
            labels = remote_cache["label"]

        for label in labels:
            logger.info("SYNC %s", label)
            rseries = remote.get(label, remote_cache)
            lseries = self.get(label, local_cache)
            if lseries.schema != rseries.schema:
                msg = f'Unable to pull label "{label}", incompatible schema.'
                raise ValueError(msg)
            lseries.pull(rseries)

    def push(self, remote, *labels):
        return remote.pull(self, *labels)

    def create(self, schema, *labels, raise_if_exists=True):
        schema_dump = schema.dump()
        self.label_series.write(
            {"label": labels, "schema": [schema_dump] * len(labels)}
        )

        res = [self.series(l, schema) for l in labels]
        if len(labels) == 1:
            return res[0]
        return res

    def search(self, label=None):
        if label:
            start = stop = (label,)
        else:
            start = stop = None
        qr = self.label_series[start:stop] @ {"closed": "both"}
        return qr.frame()

    def get(self, label, from_frm=None):
        if from_frm:
            frm = from_frm.index_slice([label], [label], closed="both")
        else:
            frm = self.search(label)

        if frm.empty:
            return None
        schema = Schema.loads(frm["schema"][-1])
        return self.series(label, schema)

    def series(self, label, schema):
        digest = hexdigest(label.encode())
        folder, filename = hashed_path(digest)
        series = Series(
            label, schema, self.series_pod / folder / filename, self.segment_pod
        )
        return series

    def squash(self):
        return self.label_series.squash()

    def delete(self, *labels):
        # Create a frame with all the existing labels contained
        # between max and min of labels
        start, stop = min(labels), max(labels)
        frm = self.label_series[start:stop].frame(closed="both")
        # Keep only labels not given as argument
        items = [(l, s) for l, s in zip(frm["label"], frm["schema"]) if l not in labels]
        if len(items) == 0:
            new_frm = self.schema.cast()
        else:
            keep_labels, keep_schema = zip(*items)
            new_frm = {
                "label": keep_labels,
                "schema": keep_schema,
            }
        # Write result to db
        self.label_series.write(new_frm, start=start, stop=stop, root=True)

    def refresh(self):
        self.label_series.refresh()

    def gc(self, soft=True):
        """
        Loop on all series, collect all used digests, and delete obsolete
        ones. If soft if true, obsolete revision are moved to an
        archive location. If soft is false, obsolete revisions are deleted.
        """
        frm = self.search()
        labels = set(frm["label"])
        series = (self.get(l, frm) for l in labels)
        per_series = (s.digests() for s in series)
        active_digests = set(chain.from_iterable(per_series))
        active_digests.update(self.label_series.digests())
        count = 0
        for filename in self.segment_pod.walk():
            digest = filename.replace("/", "")
            if digest not in active_digests:
                count += 1
                self.segment_pod.rm(filename)

        return count
