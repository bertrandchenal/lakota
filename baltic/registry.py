from itertools import chain
from time import time

from .changelog import phi
from .pod import POD
from .schema import Schema
from .series import Series
from .utils import hashed_path, hexdigest

# Idea: "package" a bunch of writes in a Zip/Tar and send the
# archive on s3


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

    def clear(self):
        self.pod.clear()

    def clone(self, remote, label, shallow=False):
        # TODO if shallow -> should be combined with a lazy cachedpod
        rseries = remote.get(label)
        self.create(rseries.schema, label)
        series = self.get(label)
        series.clone(rseries, shallow=shallow)

    def create(self, schema, *labels, raise_if_exists=True):
        new_series = []
        for label in sorted(labels):
            current = self.search(label)
            if not current.empty():
                if not raise_if_exists:
                    continue
                raise ValueError('Label "{label}" already exists')
            # Save a frame of size one
            self.schema_series.write({"label": [label], "schema": [schema.as_dict()]})
            new_series.append(self.series(label, schema))
        return new_series

    def search(self, label=None):
        # TODO use numexp expr to push down filter to Series.read
        if label:
            start = end = (label,)
        else:
            start = end = None
        # [XXX] add cache on schema_series ?
        frm = self.schema_series.read(start=start, end=end)
        return frm

    def get(self, label, from_frm=None):
        if from_frm:
            frm = from_frm.index_slice([label], [label], closed="both")
        else:
            frm = self.search(label)

        if frm.empty():
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
        frm = self.schema_series.read()
        for label in labels:
            frm = frm.remove_slice([label], [label], closed="both")
        commit = self.schema_series.write(frm, parent_commit=phi)
        self.schema_series.truncate(commit.path)

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
