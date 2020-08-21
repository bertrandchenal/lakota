from itertools import chain

from .changelog import phi
from .pod import POD
from .schema import Schema
from .series import Series
from .utils import hashed_path, hexdigest



class Registry:
    """
    Use a Series object to store all the series labels
    """

    schema = Schema(["label:str", "schema:O"])

    def __init__(self, uri=None, pod=None, lazy=False):
        # TODO add a repo and move all this pod setup in it
        self.pod = pod or POD.from_uri(uri, lazy=lazy)
        self.segment_pod = self.pod / "segment"
        self.schema_series = Series(
            "__schema_series__", self.schema, self.pod / "registry", self.segment_pod
        )
        self.series_pod = self.pod / "series"

    def pull(self, remote, *labels):
        local_cache = self.search()
        remote_cache = remote.search()

        for label in labels:
            rseries = remote.get(label, remote_cache)
            lseries = self.get(label, local_cache)
            if lseries is None:
                lseries = self.create(rseries.schema, label)
            elif lseries.schema != rseries.schema:
                msg = f'Unable to pull label "{label}", incompatible schema.'
                raise ValueError(msg)
            lseries.pull(rseries)

    def push(self, remote, *labels):
        return remote.pull(self, *labels)

    def create(self, schema, *labels, raise_if_exists=True):
        new_series = []
        for label in sorted(labels):
            current = self.search(label)
            if not current.empty:
                if not raise_if_exists:
                    continue
                raise ValueError('Label "{label}" already exists')
            # Save a frame of size one
            self.schema_series.write({"label": [label], "schema": [schema.as_dict()]})
            new_series.append(self.series(label, schema))
        if len(labels) == 1:
            return new_series[0]
        return new_series

    def search(self, label=None):
        if label:
            start = stop = (label,)
        else:
            start = stop = None
        # [XXX] add cache on schema_series ?
        frm = self.schema_series.read(start=start, stop=stop)
        return frm

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
        self.schema_series.squash()

    def delete(self, *labels):
        start, stop = min(labels), max(labels)
        frm = self.schema_series.read(start, stop)
        items = [(l, s) for l, s in zip(frm["label"], frm["schema"]) if l not in labels]
        keep_labels, keep_schema = zip(*items)
        new_frm = {
            "label": keep_labels,
            "schema": keep_schema,
        }
        self.schema_series.write(new_frm, start=start, stop=stop, parent_commit=phi)

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
        active_digests.update(self.schema_series.digests())
        count = 0
        for filename in self.segment_pod.walk():
            digest = filename.replace("/", "")
            if digest not in active_digests:
                count += 1
                self.segment_pod.rm(filename)

        return count
