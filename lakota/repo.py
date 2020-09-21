import re
from concurrent.futures import ThreadPoolExecutor
from itertools import chain

from .changelog import Changelog, phi
from .pod import POD
from .schema import Schema
from .series import KVSeries, Series
from .utils import hashed_path, hexdigest, logger

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

    def pull(self, remote, *labels):
        # Pull schema
        logger.info("SYNC labels")
        self.label_series.pull(remote.label_series)
        # Extract frames
        local_cache = self.search()
        remote_cache = remote.search()

        if not labels:
            labels = remote_cache["label"]

        with ThreadPoolExecutor(4) as pool:
            for label in labels:
                logger.info("SYNC %s", label)
                rseries = remote.get(label, remote_cache)
                lseries = self.get(label, local_cache)
                if lseries.meta != rseries.meta:
                    msg = f'Unable to pull label "{label}", incompatible meta-info.'
                    raise ValueError(msg)
                pool.submit(lseries.pull, rseries)

    def push(self, remote, *labels):
        return remote.pull(self, *labels)

    def ls(self):
        return self.search()["label"]

    def create(self, meta, *labels, raise_if_exists=True):
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
        return qr.frame()

    def get(self, label, from_frm=None):
        if from_frm:
            frm = from_frm.index_slice([label], [label], closed="both")
        else:
            frm = self.search(label)

        if frm.empty:
            return None
        meta = frm["meta"][-1]
        return self.reify(label, meta)

    def squash(self):
        # TODO pack
        return self.label_series.squash()

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


class Collection(Registry):
    def __init__(self, label, path, pod):
        self.pod = pod
        self.label = label
        self.changelog = Changelog(self.pod / path)
        super().__init__(pod, path)

    def create(self, schema, *labels, raise_if_exists=True):
        meta = {"schema": schema.dump()}
        meta = [meta] * len(labels)
        return super().create(meta, *labels, raise_if_exists=raise_if_exists)

    def series(self, name):
        return self.get(name)

    def reify(self, name, meta):
        schema = Schema.loads(meta["schema"])
        return Series(name, schema, self.pod, self.changelog)


class Repo(Registry):
    def __init__(self, uri=None, pod=None):
        pod = pod or POD.from_uri(uri)
        folder, filename = hashed_path(phi)
        super().__init__(pod, folder / filename)

    def collection(self, name):
        return self.get(name)

    def create(self, *labels, raise_if_exists=True):
        meta = []
        for label in labels:
            key = label.encode()
            digest = hexdigest(key)
            folder, filename = hashed_path(digest)
            meta.append({"path": str(folder / filename)})
        return super().create(meta, *labels, raise_if_exists=raise_if_exists)

    def reify(self, name, meta):
        return Collection(name, meta["path"], self.pod)

    def gc(self, soft=True):
        """
        Loop on all series, collect all used digests, and delete obsolete
        ones. If soft if true, obsolete revision are moved to an
        archive location. If soft is false, obsolete revisions are deleted.
        """
        coll_frm = self.search()
        coll_labels = set(coll_frm["label"])

        active_digests = set(self.label_series.digests())
        for clabel in coll_labels:
            coll = self.get(clabel, from_frm=coll_frm)
            series_frm = coll.search()
            series_labels = set(series_frm["label"])
            series = (coll.get(l, series_frm) for l in series_labels)
            per_series = (s.digests() for s in series)
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
                print("DEL", filename)
                self.pod.rm(filename)

        return count
