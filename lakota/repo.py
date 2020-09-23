import re
from itertools import chain

from .changelog import Changelog, phi
from .pod import POD
from .schema import Schema, SeriesDefinition
from .series import KVSeries, Series
from .utils import Pool, hashed_path, hexdigest, logger

LABEL_RE = re.compile("^[a-zA-Z0-9-_\.]+$")


# TODO add "tag" series to be able to tag revisions/series/collections


class Registry:
    """
    Use a Series object to store collections or series labels
    """

    schema = Schema(["label str*", "meta O"])

    def __init__(self, pod, path):
        self.pod = pod
        self.changelog = Changelog(self.pod / path)
        self.label_series = KVSeries(
            ":registry:", self.schema, pod=pod, changelog=self.changelog
        )

    def ls(self):
        return (item.label for item in self.search())

    def __iter__(self):
        return self.ls()

    def push(self, remote, *labels):
        return remote.pull(self, *labels)

    def _create(self, meta, *labels, raise_if_exists=True):
        for label in labels:
            if not LABEL_RE.match(label):
                raise ValueError(f'Invalid label "{label}"')
        self.label_series.write({"label": labels, "meta": meta})
        res = [self.reify(l, m) for l, m in zip(labels, meta)]
        if len(labels) == 1:
            return res[0]
        return res

    def search(self, label=None):
        if label:
            start = stop = (label,)
        else:
            start = stop = None
        qr = self.label_series[start:stop] @ {"closed": "both"}
        frm = qr.frame()  # XXX cache frame and use frm.slice to filter
        for l in frm["label"]:
            yield self.get(l, frm)

    def get(self, label, from_frm=None):
        if from_frm:
            frm = from_frm.index_slice([label], [label], closed="both")
        else:
            qr = self.label_series[label:label] @ {"closed": "both"}
            frm = qr.frame()  # XXX cache frame and use frm.slice to filter

        if frm.empty:
            return None
        meta = frm["meta"][-1]
        return self.reify(label, meta)

    def delete(self, *labels):
        # Create a frame with all the existing labels contained
        # between max and min of labels
        start, stop = min(labels), max(labels)
        frm = self.label_series[start:stop].frame(closed="both")
        # Keep only labels not given as argument
        items = [(l, s) for l, s in zip(frm["label"], frm["meta"]) if l not in labels]
        if len(items) == 0:
            new_frm = self.schema.cast()
        else:
            keep_labels, keep_meta = zip(*items)
            new_frm = {
                "label": keep_labels,
                "meta": keep_meta,
            }
        # Write result to db
        self.label_series.write(new_frm, start=start, stop=stop, root=True)

    def refresh(self):
        self.label_series.refresh()

    def revisions(self):
        return self.label_series.revisions()

    def truncate(self, *skip):
        self.changelog.pod.clear(*skip)

    def __truediv__(self, name):
        return self.get(name)


class Collection(Registry):
    def __init__(self, label, path, pod):
        self.pod = pod
        self.label = label
        self.changelog = Changelog(self.pod / path)
        super().__init__(pod, path)

    def create_series(self, schema, *labels, raise_if_exists=True):
        meta = {"schema": schema.dump()}
        meta = [meta] * len(labels)
        return super()._create(meta, *labels, raise_if_exists=raise_if_exists)

    def series(self, name):
        return self.get(name)

    def reify(self, name, meta):
        schema = Schema.loads(meta["schema"])
        return Series(name, schema, self.pod, self.changelog)

    def __add__(self, other):
        if not isinstance(other, SeriesDefinition):
            raise ValueError("Incorrect invocation")
        return self.create_series(other.schema, *other.labels)

    def squash(self):
        """
        Remove all past revisions, collapse history into one or few large
        frames.
        """
        # TODO accumulate all writes in one commit

        step = 500_000
        all_series = list(self.search())
        # Re-write registry
        commits = [
            self.label_series.write(frm, root=True)
            for frm in self.label_series.paginate(step)
        ]
        for series in all_series:
            # Re-write each series
            commits.extend(
                series.write(frm, root=True) for frm in series.paginate(step)
            )
        self.truncate(*(c.path for c in commits))
        return commits

    def pull(self, remote, *labels):
        assert isinstance(remote, Collection), "A Collection instance is required"

        # Pull schema
        self.label_series.pull(remote.label_series)
        # Extract frames
        local_cache = {l.label: l for l in self.search()}
        remote_cache = {r.label: r for r in remote.search()}

        if not labels:
            labels = remote_cache.keys()

        with Pool() as pool:
            for label in labels:
                logger.info("Sync series: %s", label)
                rseries = remote_cache[label]
                lseries = local_cache[label]
                if lseries.schema != rseries.schema:
                    msg = f'Unable to pull label "{label}", incompatible meta-info.'
                    raise ValueError(msg)
                pool.submit(lseries.pull, rseries)


class Repo(Registry):
    def __init__(self, uri=None, pod=None):
        pod = pod or POD.from_uri(uri)
        folder, filename = hashed_path(phi)
        super().__init__(pod, folder / filename)

    def collection(self, name):
        return self.get(name)

    def create_collection(self, *labels, raise_if_exists=True):
        meta = []
        for label in labels:
            key = label.encode()
            digest = hexdigest(key)
            folder, filename = hashed_path(digest)
            meta.append({"path": str(folder / filename)})
        return super()._create(meta, *labels, raise_if_exists=raise_if_exists)

    def __add__(self, label):
        return self.create_collection(label, raise_if_exists=False)

    def reify(self, name, meta):
        return Collection(name, meta["path"], self.pod)

    def pull(self, remote, *labels):
        assert isinstance(remote, Repo), "A Repo instance is required"
        # Pull schema
        self.label_series.pull(remote.label_series)
        # Extract frames
        local_cache = {l.label: l for l in self.search()}
        remote_cache = {r.label: r for r in remote.search()}
        if not labels:
            labels = remote_cache.keys()

        with Pool() as pool:
            for label in labels:
                logger.info("Sync collection: %s", label)
                rcoll = remote_cache[label]
                lcoll = local_cache[label]
                pool.submit(lcoll.pull, rcoll)

    def squash(self):
        """
        Remove all past revisions, collapse history into one or few large
        frames.
        """
        # TODO accumulate all writes in one commit

        step = 500_000
        # Re-write registry
        commits = [
            self.label_series.write(frm, root=True)
            for frm in self.label_series.paginate(step)
        ]
        self.truncate(*(c.path for c in commits))
        return commits

    def gc(self):
        """
        Loop on all series, collect all used digests, and delete obsolete
        ones.
        """
        collections = self.search()

        active_digests = set(self.label_series.digests())
        for coll in collections:
            all_series = coll.search()
            per_series = (s.digests() for s in all_series)
            active_digests.update(chain.from_iterable(per_series))
            active_digests.update(coll.label_series.digests())

        count = 0
        for filename in self.pod.walk(max_depth=3):
            if self.pod.isdir(filename):
                # A directory contains a changelog
                continue
            digest = filename.replace("/", "")
            if digest not in active_digests:
                count += 1
                self.pod.rm(filename)

        return count
