import re
from itertools import chain

from .changelog import Changelog, phi
from .pod import POD
from .schema import Schema
from .series import KVSeries, Series
from .utils import Pool, hashed_path, hexdigest, logger

LABEL_RE = re.compile("^[a-zA-Z0-9-_\.]+$")


class Collection:
    def __init__(self, label, schema, path, repo):
        self.repo = repo
        self.pod = repo.pod
        self.schema = schema
        self.label = label
        self.changelog = Changelog(self.pod / path)

    def series(self, label):
        if not LABEL_RE.match(label):
            raise ValueError(f'Invalid label "{label}"')
        cls = KVSeries if self.schema.kind == "KVSeries" else Series
        return cls(label, self.schema, self.pod, self.changelog)

    def __truediv__(self, name):
        return self.series(name)

    def __iter__(self):
        return iter(self.ls())

    def ls(self):
        revs = self.changelog.walk()
        return sorted(set(r["label"] for r in revs))

    def pack(self):
        return self.changelog.pack()

    def delete(self, *labels):
        if not labels:
            return
        keep = lambda rev: rev["label"] not in labels
        self.changelog.pack(keep)

    def refresh(self):
        self.changelog.refresh()

    def squash(self, archive=True):
        """
        Remove all past revisions, collapse history into one or few large
        frames.
        """

        # Accumulate commit info in a list
        step = 500_000
        all_labels = self.ls()
        batch = []
        for series in (self.series(l) for l in all_labels):
            # Re-write each series
            for frm in series.paginate(step):
                res = series.write(frm, batch=True)
                # res is either None, either a tuple formed by a dict
                # containing commit payload and a key
                if res is None:
                    import pdb

                    pdb.set_trace()
                    continue
                batch.append(res)
        if len(batch) == 0:
            return

        # Build key
        all_revs, keys = list(zip(*batch))
        if len(keys) == 1:
            (key,) = keys
        else:
            key = hexdigest(*(k.encode() for k in keys))
        # Create combined commit
        commit = self.changelog.commit(all_revs, key=key, force_parent=phi)

        if archive:
            # TODO make sure similar commit does not already exists
            # (when squash is called several time without intermediate
            # changes)
            archive_pod = self.repo.archive(self).changelog.pod
            for path in self.changelog:
                if path == commit.path:
                    continue
                data = self.changelog.pod.read(path)
                archive_pod.write(path, data)

        self.changelog.pod.clear(commit.path)
        return commit

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
    schema = Schema(["label str*", "meta O"], kind="KVSeries")

    def __init__(self, uri=None, pod=None):
        pod = pod or POD.from_uri(uri)
        folder, filename = hashed_path(phi)
        self.pod = pod
        path = folder / filename
        self.registry = Collection("registry", self.schema, path, self)
        self.collection_series = self.registry.series("collection")

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

    def collection(self, label, from_frm=None, mode=None):
        if mode is None:
            series = self.collection_series
        elif mode == "archive":
            series = self.registry.series("archive")
        else:
            raise ValueError(f'Unexpected mode: "{mode}"')

        if not from_frm:
            from_frm = series.frame()
        frm = from_frm.index_slice([label], [label], closed="both")

        if frm.empty:
            return None
        meta = frm["meta"][-1]
        return self.reify(label, meta)

    def create_collection(self, schema, *labels, raise_if_exists=True, mode=None):
        assert isinstance(
            schema, Schema
        ), "The schema parameter must be an instance of lakota.Schema"
        meta = []
        schema_dump = schema.dump()

        if mode is None:
            series = self.collection_series
        elif mode == "archive":
            series = self.registry.series("archive")
        else:
            raise ValueError(f'Unexpected mode "{mode}"')

        for label in labels:
            if not LABEL_RE.match(label):
                raise ValueError(f'Invalid label "{label}"')
            key = label.encode()
            # Use digest to create collection folder (based on mode and label)
            digest = hexdigest(key)
            if mode:
                digest = hexdigest(digest.encode(), mode.encode())
            folder, filename = hashed_path(digest)
            meta.append({"path": str(folder / filename), "schema": schema_dump})

        series.write({"label": labels, "meta": meta})
        res = [self.reify(l, m) for l, m in zip(labels, meta)]
        if len(labels) == 1:
            return res[0]
        return res

    def reify(self, name, meta):
        schema = Schema.loads(meta["schema"])
        return Collection(name, schema, meta["path"], self)

    def archive(self, collection):
        label = collection.label
        archive = self.collection(label, mode="archive")
        if archive:
            return archive
        return self.create_collection(collection.schema, label, mode="archive")

    def delete(self, *labels):
        self.collection_series.delete(*labels)

    def refresh(self):
        self.collection_series.refresh()

    def revisions(self):
        return self.collection_series.revisions()

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
        return self.registry.squash()

    def pack(self):
        return self.registry.pack()

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
