from contextlib import contextmanager
from itertools import chain

from .changelog import Changelog, phi, zero_hash
from .pod import POD
from .schema import Schema
from .series import KVSeries, Series
from .utils import Pool, hashed_path, hexdigest, logger


class Collection:
    def __init__(self, label, schema, path, repo):
        self.repo = repo
        self.pod = repo.pod
        self.schema = schema
        self.label = label
        self.changelog = Changelog(self.pod / path)

    def series(self, label):
        label = label.strip()
        if len(label) == 0:
            raise ValueError(f"Invalid label")
        cls = KVSeries if self.schema.kind == "kv" else Series
        return cls(label, self)

    def __truediv__(self, name):
        return self.series(name)

    def __iter__(self):
        return iter(self.ls())

    def ls(self):
        revs = self.changelog.walk()
        labels = set()
        for r in revs:
            label = r["label"]
            if r.get("tombstone"):
                labels.discard(label)
            else:
                labels.add(label)
        return sorted(labels)

    def pack(self):
        return self.changelog.pack()

    def delete(self, *labels):
        if not labels:
            return
        with self.batch(phi) as batch:
            for label in labels:
                self.series(label).delete(batch=batch)

    def refresh(self):
        self.changelog.refresh()

    def push(self, remote, *labels):
        return remote.pull(self, *labels)

    def pull(self, remote):
        """
        Pull remote series into self
        """
        assert isinstance(remote, Collection), "A Collection instance is required"

        # TODO use local storage as cache for remote (when reading revisions)
        local_digs = set()
        for revision in self.revisions():
            local_digs.update(revision.get("digests", []))
        self.changelog.pull(remote.changelog)
        self.refresh()
        # XXX optionaly isolate local path not detected in the loop
        # here-under (and return them at the end to let Repo.pull do
        # the deletions) (but what about local history?)
        sync = lambda path: self.pod.write(path, remote.pod.read(path))
        with Pool() as pool:
            for revision in self.revisions():
                for dig in revision.get("digests", []):
                    if dig in local_digs:
                        continue
                    folder, filename = hashed_path(dig)
                    path = folder / filename
                    pool.submit(sync, path)

    def revisions(self):
        return self.changelog.walk()

    def squash(self, archive=True):
        """
        Remove all past revisions, collapse history into one or few large
        frames.
        """
        # TODO should be able to be run on a bunch of commit (all
        # commits before or after a given point in time) and leave
        # others untouched

        # Accumulate commit info in a list
        step = 500_000
        all_labels = self.ls()
        batch = []
        with self.batch(phi) as batch:
            for label in all_labels:
                # Re-write each series
                series = self / label
                for frm in series.paginate(step):
                    series.write(frm, batch=batch)

        if not batch.commit:
            return

        if archive:
            # TODO prefix/suffix schema with an _wtime column to keep
            # trace of revisions (to be able to write larger segments
            # and squash changelog)
            archive_pod = self.repo.archive(self).changelog.pod
            # TODO use pack instead (and give archive pod as arg)
            for path in self.changelog:
                if path == batch.commit.path:
                    continue
                data = self.changelog.pod.read(path)
                archive_pod.write(path, data)

        self.changelog.pod.clear(batch.commit.path)
        return batch.commit

    @contextmanager
    def batch(self, force_parent=None):
        b = Batch(self, force_parent)
        yield b
        b.flush()


class Batch:
    def __init__(self, collection, force_parent=None):
        self.collection = collection
        self._commits = []
        self.commit = None
        self.force_parent = force_parent

    def append(self, rev_info, key):
        self._commits.append((rev_info, key))

    def flush(self):
        if len(self._commits) == 0:
            return
        # Build key
        all_revs, keys = list(zip(*self._commits))
        if len(keys) == 1:
            (key,) = keys
        else:
            key = hexdigest(*(k.encode() for k in keys))
        # Create combined commit
        changelog = self.collection.changelog

        self.commit = changelog.commit(
            all_revs, key=key, force_parent=self.force_parent
        )


class Repo:
    schema = Schema(["label str*", "meta O"], kind="kv")

    def __init__(self, uri=None, pod=None):
        pod = pod or POD.from_uri(uri)
        folder, filename = hashed_path(zero_hash)
        self.pod = pod
        path = folder / filename
        # TODO harmonize code between 'normal' collection and archive ones
        self.registry = Collection("registry", self.schema, path, self)
        self.collection_series = self.registry.series("collection")

    def ls(self):
        return (item.label for item in self.search())

    def __iter__(self):
        return self.ls()

    def push(self, remote, *labels):
        return remote.pull(self, *labels)

    def search(self, label=None, mode=None):
        if label:
            start = stop = (label,)
        else:
            start = stop = None
        if mode == "archive":
            series = self.registry.series("archive")
        else:
            series = self.collection_series
        qr = series[start:stop] @ {"closed": "both"}

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

        # TODO validate columns(can not start with a _)

        if mode is None:
            series = self.collection_series
        elif mode == "archive":
            series = self.registry.series("archive")
        else:
            raise ValueError(f'Unexpected mode "{mode}"')

        for label in labels:
            label = label.strip()
            if len(label) == 0:
                raise ValueError(f"Invalid label")

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
        to_remove = []
        for l in labels:
            clct = self.collection(l)
            if not clct:
                continue
            to_remove.append(clct.changelog.pod)
        self.collection_series.delete(*labels)
        for pod in to_remove:
            try:
                pod.rm(".", recursive=True)
            except FileNotFoundError:
                continue

    def refresh(self):
        self.collection_series.refresh()

    def revisions(self):
        return self.collection_series.revisions()

    def pull(self, remote, *labels):
        assert isinstance(remote, Repo), "A Repo instance is required"
        # Pull registry
        self.registry.pull(remote.registry)
        # Extract frames
        local_cache = {l.label: l for l in self.search()}
        remote_cache = {r.label: r for r in remote.search()}
        if not labels:
            labels = remote_cache.keys()

        with Pool() as pool:
            for label in labels:
                logger.info("Sync collection: %s", label)
                r_clct = remote_cache[label]
                if not label in local_cache:
                    l_clct = self.create_collection(r_clct.schema, label)
                else:
                    l_clct = local_cache[label]
                    if l_clct.schema != r_clct.schema:
                        msg = (
                            f'Unable to sync collection "{label}",'
                            "incompatible meta-info."
                        )
                        raise ValueError(msg)
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
        # XXX remove old revisions (anything before a pack commit)

        active_digests = set()
        for mode in (None, "archive"):
            coll_series = (
                self.collection_series if mode is None else self.registry.series(mode)
            )
            active_digests.update(coll_series.digests())
            for clct in self.search(mode=mode):
                all_series = [clct.series(s) for s in clct]
                per_series = (s.digests() for s in all_series)
                active_digests.update(chain.from_iterable(per_series))

        base_folders = self.pod.ls()
        with Pool(8) as pool:
            for folder in base_folders:
                pool.submit(self.gc_folder, folder, active_digests)
        count = sum(pool.results)
        return count

    def gc_folder(self, folder, active_digests):
        count = 0
        for filename in self.pod.cd(folder).walk(max_depth=2):
            digest = folder + filename.replace("/", "")
            if digest not in active_digests:
                count += 1
                self.pod.rm(filename, missing_ok=True)
        return count
