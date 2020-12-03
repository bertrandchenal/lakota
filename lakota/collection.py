from collections import defaultdict
from contextlib import contextmanager

from .changelog import Changelog, phi
from .series import Commit, KVSeries, Series
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
        rev = self.changelog.leaf()
        if rev is None:
            return []
        payload = rev.read()
        ci = Commit.decode(self.schema, payload)
        return sorted(set(ci.label))

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

    def merge(self, *heads):
        revisions = list(self.changelog.log())
        # Corner cases
        if not revisions:
            return
        if not heads:
            heads = [r for r in revisions if r.is_leaf]

        if len(heads) < 2:
            return

        # Reorganise revision as child->parents dict
        ch2pr = defaultdict(list)
        for r in revisions:
            ch2pr[r.child].append(r)

        # Find common root
        root = None
        first_parents, *other_parents = [
            list(self._find_parents(h, ch2pr)) for h in heads
        ]
        for root in first_parents:
            if all(root in op for op in other_parents):
                break

        # Reify commits
        first_ci, *other_ci = [h.commit(self) for h in heads]
        root_ci = root.commit(self) if root else []
        # Pile all rows for all other commit into the first one
        for ci in other_ci:
            for pos in range(len(ci)):
                row = ci.at(pos)
                if row in first_ci or row in root_ci:
                    continue
                first_ci = first_ci.update(**row)

        # encode and commit
        payload = first_ci.encode()
        revs = self.changelog.commit(payload, parents=[h.child for h in heads])
        return revs

    @staticmethod
    def _find_parents(rev, ch2pr):
        queue = ch2pr[rev.child][:]
        while queue:
            rev = queue.pop()
            # Append children
            parents = ch2pr[rev.parent]
            queue.extend(parents)
            yield rev

    def squash(self, archive=False):
        """
        Remove all past revisions, collapse history into one or few large
        frames.
        """
        step = 500_000
        all_labels = self.ls()
        with self.batch(phi) as batch:
            for label in all_labels:
                logger.info('SQUASH label "%s"', label)
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
    def __init__(self, collection, root=False):
        self.collection = collection
        self._ci_info = []
        self.revs = None
        self.root = root

    def append(self, ci_info):
        self._ci_info.append(ci_info)

    def flush(self):
        if len(self._ci_info) == 0:
            return

        changelog = self.collection.changelog
        leaf_rev = None if self.root else changelog.leaf()
        all_ci_info = iter(self._ci_info)

        # Combine with last commit
        if leaf_rev:
            last_ci = leaf_rev.commit(self)
        else:
            label, start, stop, all_dig, length = next(all_ci_info)
            last_ci = Commit.one(
                self.collection.schema, label, start, stop, all_dig, length
            )
        for label, start, stop, all_dig, length in all_ci_info:
            last_ci = last_ci.update(label, start, stop, all_dig, length)

        # Save it
        payload = last_ci.encode()
        parent = leaf_rev.child if leaf_rev else phi
        self.revs = self.changelog.commit(payload, parents=[parent])
