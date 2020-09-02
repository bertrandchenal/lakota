import re
from itertools import chain

from .changelog import phi
from .pod import POD
from .schema import Schema
from .series import Series
from .utils import hashed_path, hexdigest, logger

LABEL_RE = re.compile("^[a-zA-Z0-9-_]+$")


class Registry:
    """
    Use a Series object to store all the series labels
    """

    schema = Schema(["label str*", "schema O"])

    def __init__(self, uri=None, pod=None):
        # TODO add a repo and move all this pod setup in it
        self.pod = pod or POD.from_uri(uri)
        self.segment_pod = self.pod / "segment"
        self.schema_series = Series(  # TODO rename into "series"
            "__schema_series__", self.schema, self.pod / "registry", self.segment_pod
        )
        self.series_pod = self.pod / "series"

    def pull(self, remote, *labels):
        local_cache = self.search()
        remote_cache = remote.search()
        # Pull schema
        self.schema_series.pull(remote.schema_series)

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
        res = []
        current_labels = self.search()["label"]
        max_label = len(current_labels) and current_labels[-1]
        bigger_labels = []
        schema_dump = schema.dump()
        for label in sorted(labels):
            res.append(self.series(label, schema))
            # Make sure label is valid
            if not LABEL_RE.match(label):
                raise ValueError(f'Invalid label: "{label}"')
            # Optionally raise an exception if label exists
            if label in current_labels:
                if not raise_if_exists:
                    continue
                raise ValueError('Label "{label}" already exists')

            if not max_label or label > max_label:
                # Accumulate labels in order to batch the write
                bigger_labels.append(label)
                continue
            else:
                # Write a frame of size one
                self.schema_series.write({"label": [label], "schema": [schema_dump]})

        # Create one write with all the labels that are bigger than the current range
        if bigger_labels:
            self.schema_series.write(
                {"label": bigger_labels, "schema": [schema_dump] * len(bigger_labels)}
            )

        if len(labels) == 1:
            return res[0]
        return res

    def search(self, label=None):
        if label:
            start = stop = (label,)
        else:
            start = stop = None
        # [XXX] add cache on schema_series ?
        frm = self.schema_series.closed("both")[start:stop]
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
        # Create a frame with all the existing labels contained
        # between max and min of labels
        start, stop = min(labels), max(labels)
        frm = self.schema_series.closed("both")[start:stop]

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
        self.schema_series.write(new_frm, start=start, stop=stop)

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
