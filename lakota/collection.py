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

The `lakota.collection.Collection.multi` method returns a contect manager that will provide atomic
(and faster) writes across several series
```python
with clct.multi():
    for label, df in ...:
        series = clct / label
        series.write(df)
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

import threading
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from itertools import chain

from .batch import Batch
from .changelog import Changelog
from .commit import Commit
from .series import KVSeries, Series
from .utils import Pool, hashed_path, logger, settings

__all__ = ["Collection", "Batch"]


class Collection:
    def __init__(self, label, schema, path, repo):
        self.repo = repo
        self.pod = repo.pod
        self.schema = schema
        self.label = label
        self.changelog = Changelog(self.pod / path)
        self._local = threading.local()
        self._local.batch = None

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

    def rename(self, from_label, to_label):
        leaf_rev = self.changelog.leaf()
        if not leaf_rev:
            return

        ci = leaf_rev.commit(self)
        ci = ci.rename_label(from_label, to_label)
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
            return []
        if not heads:
            heads = [r for r in revisions if r.is_leaf]

        # We may have multiple revision pointing to the same child
        # (aka a previous commit). No need to merge again.
        if len(set(r.digests.child for r in heads)) < 2:
            return []

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
        self.batch = True
        for ci in other_ci:
            for pos in range(len(ci)):
                row = ci.at(pos)
                # Skip existing rows
                if row in first_ci or row in root_ci:
                    continue

                # Re-apply row
                closed = row["closed"]
                if closed == "b":
                    # Closed commit can be applied as-is
                    first_ci = first_ci.update(**row)
                else:
                    # Non-closed: we read and rewrite
                    series = self / row["label"]
                    frm = series.frame(
                        start=row["start"], stop=row["stop"], closed=closed, from_ci=ci
                    )
                    ci_info = series.write(
                        frm
                    )  # since batch is true series simply returns info
                    first_ci = first_ci.update(*ci_info)

        # encode and commit
        payload = first_ci.encode()
        revs = self.changelog.commit(payload, parents=set(h.child for h in heads))
        self.batch = False
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

    def squash(self, pack=True, trim=True):
        """
        Remove past revisions, collapse history into one or few large
        frames. Returns newly created revisions.

        If `pack` is False, no new revision is created. If set to
        True, the new revisions created are a complete rewrite of the
        collection, the goal being to defragment the multiple arrays
        constituting the different series.

        If `trim` is True, all revisions except the last one are
        removed.  If set to False, the full history is kept. If set to
        a datetime, all the revision older than the given value will be
        deleted, keeping the recent history.
        """
        logger.info('Squash collection "%s"', self.label)

        # Read existing revisions
        if trim:
            before = trim if isinstance(trim, datetime) else None
            revs = self.changelog.log(before=before)
        else:
            revs = []

        if not pack:
            # Simply remove older commit
            self.changelog.pod.rm_many([r.path for r in revs[:-1]])
            self.changelog.refresh()
            return []

        # Rewrite each series, based on `step` size arrays
        all_labels = self.ls()
        leaf = self.changelog.leaf()
        commit = leaf and leaf.commit(self)
        # TODO run in parallel
        with self.multi() as batch:
            for label in all_labels:
                logger.info('Squash series "%s"', label)
                # Re-write each series. We use _find_squash_start to
                # fast-forward in the series (we bet on the fact that
                # most series are append-only)
                start = self._find_squash_start(commit, label)
                series = self / label
                for frm in series.paginate(start=start):
                    series.write(frm)

        # Remove old revisions
        to_remove = [r.path for r in revs]
        if not batch.revs:
            # No new revision created, keep the last one
            to_remove = to_remove[:-1]
        self.changelog.pod.rm_many(to_remove)

        self.changelog.refresh()
        return batch.revs

    def _find_squash_start(self, commit, label):
        """
        Find the first "small" segment , and return its start values.
        """
        start = None
        rows = list(commit.match(label))
        if len(rows) < 4:
            return rows[-1]["stop"]

        total_len = sum(row["length"] for row in rows)
        threshold = min(settings.page_len, total_len / 4)
        for row in rows:
            if row["length"] < threshold:
                break
            start = row["start"]
        return start

    def digests(self):
        for rev in self.changelog.log():
            ci = rev.commit(self)
            digs = set(chain.from_iterable(ci.digest.values()))
            # return only digest not already embedded in the commit
            digs = digs - set(ci.embedded)
            yield from digs

    @contextmanager
    def multi(self, root=None):
        b = Batch(self, root)
        self.batch = b
        yield b
        b.flush()
        self.batch = None

    @property
    def batch(self):
        return self._local.batch

    @batch.setter
    def batch(self, b):
        self._local.batch = b
