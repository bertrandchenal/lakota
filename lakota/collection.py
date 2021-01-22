"""
## Read and Writes Series

A collection is instantiated from a `lakota.repo.Repo` object (see `lakota.repo`):
```python
clct = repo / 'my_collection'
```

Series instantiation

```python
all_series = clct.ls()

my_series = clct.series('my_series')
# or
my_series = clct.series / 'my_series'
```

See `lakota.series` on how to use `lakota.series.Series`.

The `lakota.collection.Collection.batch` method returns a contect manager that will provide atomic
(and faster) writes across several series
```python
with clct.batch() as batch:
    for label, df in ...:
        series = clct / label
        series.write(df, batch=batch)
```

## Concurrent writes and synchronization

Collections can also be pushed/pulled and merged.

```python
clct = local_repo / 'my_collection'
remote_clct = remote_repo / 'my_collection'
clct.pull(remote_clct)
clct.merge()
```

Squash remove past revisions
```python
clct.squash()
```
"""

from collections import defaultdict
from contextlib import contextmanager
from itertools import chain

from .changelog import Changelog, phi
from .series import Commit, KVSeries, Series
from .utils import Pool, hashed_path, logger

__all__ = ["Collection", "Batch"]


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

    def delete(self, *labels):
        leaf_rev = self.changelog.leaf()
        if not leaf_rev:
            return

        ci = leaf_rev.commit(self)
        ci = ci.delete_labels(labels)
        parent = leaf_rev.child
        payload = ci.encode()
        return self.changelog.commit(payload, parents=[parent])

    def refresh(self):
        self.changelog.refresh()

    def push(self, remote, *labels):
        return remote.pull(self, *labels)

    def pull(self, remote):
        """
        Pull remote series into self
        """
        assert isinstance(remote, Collection), "A Collection instance is required"

        local_digs = set(self.digests())
        remote_digs = set(remote.digests())
        sync = lambda path: self.pod.write(path, remote.pod.read(path))
        with Pool() as pool:
            for dig in remote_digs:
                if dig in local_digs:
                    continue
                folder, filename = hashed_path(dig)
                path = folder / filename
                pool.submit(sync, path)

        self.changelog.pull(remote.changelog)

    def merge(self, *heads):
        revisions = self.changelog.log()
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

        # Reify commits, changelog.log is a depth first traversal, so
        # the first head is also the oldest branch.
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

    def squash(self, fast=False):
        """
        Remove all past revisions, collapse history into one or few large
        frames. Returns newly created revisions.
        """
        if fast:
            # Simply remove older commit
            leaf = self.changelog.leaf()
            if leaf:
                self.changelog.pod.clear(leaf.path)
                self.changelog.refresh()
            return

        # Rewrite each series, based on `step` size arrays
        step = 500_000
        all_labels = self.ls()
        # TODO run in parallel
        with self.batch() as batch:
            for label in all_labels:
                logger.info('SQUASH label "%s"', label)
                # Re-write each series
                series = self / label
                for frm in series.paginate(step):
                    series.write(frm, batch=batch)

        if batch.revs:
            revs = batch.revs
        else:
            leaf = self.changelog.leaf()
            revs = [leaf] if leaf is not None else []
        skip = [r.path for r in revs]
        self.changelog.pod.clear(*skip)
        self.changelog.refresh()
        return batch.revs

    def digests(self):
        for rev in self.changelog.log():
            ci = rev.commit(self)
            yield from chain.from_iterable(ci.digest.values())

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

    def extend(self, *other_batches):
        for b in other_batches:
            self._ci_info.extend(b._ci_info)

    def flush(self):
        if len(self._ci_info) == 0:
            return

        changelog = self.collection.changelog
        leaf_rev = None if self.root else changelog.leaf()
        all_ci_info = iter(self._ci_info)

        # Combine with last commit
        if leaf_rev:
            last_ci = leaf_rev.commit(self.collection)
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
        self.revs = self.collection.changelog.commit(payload, parents=[parent])
