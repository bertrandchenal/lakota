import re
from itertools import chain

from .changelog import Changelog, phi
from .pod import POD
from .schema import Schema
from .series import KVSeries, Series
from .utils import Pool, hashed_path, hexdigest, logger

LABEL_RE = re.compile("^[a-zA-Z0-9-_\.]+$")

# TODO add "tag" series to be able to tag revisions/series/collections


class Collection:
    def __init__(self, label, schema, path, pod):
        self.pod = pod
        self.schema = schema
        self.label = label
        self.changelog = Changelog(self.pod / path)

    def series(self, label):
        if not LABEL_RE.match(label):
            raise ValueError(f'Invalid label "{label}"')
        return Series(label, self.schema, self.pod, self.changelog)

    def __truediv__(self, name):
        return self.series(name)

    def __iter__(self):
        return iter(self.ls())

    def ls(self):
        revs = self.changelog.walk()
        return sorted(set(r["label"] for r in revs))

    def pack(self):
        self.changelog.pack()

    def delete(self, *labels):
        if not labels:
            return
        keep = lambda rev: rev["label"] not in labels
        self.changelog.pack(keep)

    def refresh(self):
        self.changelog.refresh()

    def squash(self):
        """
        Remove all past revisions, collapse history into one or few large
        frames.
        """
        # TODO accumulate all writes in one commit

        step = 500_000
        all_labels = self.ls()
        commits = []
        for series in (self.series(l) for l in all_labels):
            # Re-write each series
            commits.extend(
                series.write(frm, root=True) for frm in series.paginate(step)
            )

        self.changelog.pod.clear(*(c.path for c in commits))
        return commits

    def push(self, remote, *labels):
        return remote.pull(self, *labels)

    def pull(self, remote, *labels):
        assert isinstance(remote, Collection), "A Collection instance is required"

        # Extract labels
        # local_cache = {l: self.series(l) for l in self}
        remote_cache = {l: remote.series(l) for l in remote}

        if not labels:
            labels = remote_cache.keys()

        with Pool() as pool:
            for label in labels:
                logger.info("Sync series: %s", label)
                rseries = remote_cache[label]
                lseries = self / label
                if lseries.schema != rseries.schema:
                    msg = f'Unable to pull label "{label}", incompatible meta-info.'
                    raise ValueError(msg)
                pool.submit(lseries.pull, rseries)

    def revisions(self):
        return self.changelog.walk()


class Repo:
    schema = Schema(["label str*", "meta O"])

    def __init__(self, uri=None, pod=None):
        pod = pod or POD.from_uri(uri)
        folder, filename = hashed_path(phi)
        self.pod = pod
        self.changelog = Changelog(self.pod / folder / filename)
        self.collection_series = KVSeries(
            "collection", self.schema, pod=pod, changelog=self.changelog
        )

    def ls(self):
        return (item.label for item in self.search())

    def __iter__(self):
        return self.ls()

    def push(self, remote, *labels):
        return remote.pull(self, *labels)

    def search(self, label=None):
        if label:
            start = stop = (label,)
        else:
            start = stop = None
        qr = self.collection_series[start:stop] @ {"closed": "both"}
        frm = qr.frame()  # TODO re-use cache
        for l in frm["label"]:
            yield self.collection(l, frm)

    def __truediv__(self, name):
        return self.collection(name)

    def collection(self, label, from_frm=None):
        if not from_frm:
            from_frm = self.collection_series.frame()
        frm = from_frm.index_slice([label], [label], closed="both")

        if frm.empty:
            return None
        meta = frm["meta"][-1]
        return self.reify(label, meta)

    def delete(self, *labels):
        self.collection_series.delete(*labels)

    def refresh(self):
        self.collection_series.refresh()

    def revisions(self):
        return self.collection_series.revisions()

    def create_collection(self, schema, *labels, raise_if_exists=True):
        assert isinstance(
            schema, Schema
        ), "The schema parameter must be an instance of lakota.Schema"
        meta = []
        schema_dump = schema.dump()
        for label in labels:
            if not LABEL_RE.match(label):
                raise ValueError(f'Invalid label "{label}"')
            key = label.encode()
            digest = hexdigest(key)
            folder, filename = hashed_path(digest)
            meta.append({"path": str(folder / filename), "schema": schema_dump})

        self.collection_series.write({"label": labels, "meta": meta})
        res = [self.reify(l, m) for l, m in zip(labels, meta)]
        if len(labels) == 1:
            return res[0]
        return res

    def reify(self, name, meta):
        schema = Schema.loads(meta["schema"])
        return Collection(name, schema, meta["path"], self.pod)

    def pull(self, remote, *labels):
        assert isinstance(remote, Repo), "A Repo instance is required"
        # Pull schema
        self.collection_series.pull(remote.collection_series)
        # Extract frames
        local_cache = {l.label: l for l in self.search()}
        remote_cache = {r.label: r for r in remote.search()}
        if not labels:
            labels = remote_cache.keys()

        with Pool() as pool:
            for label in labels:
                logger.info("Sync collection: %s", label)
                r_clct = remote_cache[label]
                l_clct = local_cache[label]
                pool.submit(l_clct.pull, r_clct)

    def squash(self):
        """
        Remove all past revisions, collapse history into one or few large
        frames.
        """
        # TODO accumulate all writes in one commit

        step = 500_000
        # Re-write registry
        commits = [
            self.collection_series.write(frm, root=True)
            for frm in self.collection_series.paginate(step)
        ]
        self.changelog.pod.clear(*(c.path for c in commits))
        return commits

    def pack(self):
        self.collection_series.changelog.pack()

    def gc(self):
        """
        Loop on all series, collect all used digests, and delete obsolete
        ones.
        """
        collections = self.search()

        # TODO remove old revisions (anything before a pack commit)

        active_digests = set(self.collection_series.digests())
        for clct in collections:
            all_series = [clct.series(s) for s in clct]
            per_series = (s.digests() for s in all_series)
            active_digests.update(chain.from_iterable(per_series))

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
