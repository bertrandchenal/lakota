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

Trim remove past revisions
```python
clct.trim()
```

Defrag Recombine small data files into bigger ones
```python
clct.defrag()
```
"""

from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta
from itertools import chain
from threading import Lock
from typing import Dict, Any
from warnings import warn

from .batch import Batch
from .changelog import Changelog, phi
from .commit import Commit
from .series import KVSeries, Series
from .utils import Pool, hashed_path, logger, settings

__all__ = ["Collection"]


class Collection:
    def __init__(self, label, schema, path, repo):
        self.repo = repo
        self.pod = repo.pod
        self.schema = schema
        self.label = label
        self.changelog = Changelog(self.pod / path)
        self.batch = None
        self._batch_lock = Lock()

    def series(self, label):
        label = label.strip()
        if len(label) == 0:
            raise ValueError(f"Invalid label: '{label}'")
        cls = KVSeries if self.schema.kind == "kv" else Series
        return cls(label, self)

    def __truediv__(self, name):
        return self.series(name)

    def __iter__(self):
        return (self.series(n) for n in self.ls())

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
        """
        Rename Series
        """
        leaf_rev = self.changelog.leaf()
        if not leaf_rev:
            return

        ci = leaf_rev.commit(self)
        ci = ci.rename_label(from_label, to_label)
        parent = leaf_rev.child
        payload = ci.encode()
        return self.changelog.commit(payload, parents=[parent])

    def clone(
        self,
        other_collection: "Collection",
        rename_columns: Dict[str, str] = None,
        defaults: Dict[str, Any] = None,
    ) -> Commit:
        if other_collection.changelog.leaf():
            raise ValueError("Clone can only be saved into an empty collection")

        other_schema = other_collection.schema
        leaf_rev = self.changelog.leaf()
        leaf_ci = leaf_rev.commit(self)
        all_dig = leaf_ci.digest
        for old_label, new_label in (rename_columns or {}).items():
            all_dig[new_label] = all_dig.pop(old_label)
        embedded = {}
        for col in other_schema:
            if col in all_dig:
                continue
            if col in other_schema.idx:
                raise ValueError("Can not add idx column")
            # Fill each commit row with digest & embed data of zeroes
            if defaults and col in defaults:
                default_value = defaults[col]
                values = [[default_value] * l for l in leaf_ci.length]
            else:
                values = [other_schema[col].zeroes(l) for l in leaf_ci.length]
            codec = other_schema[col].codec
            col_embedded = {}
            for v in values:
                encoded, digest = codec.encode(v, with_digest=True)
                col_embedded[digest] = encoded
            # Add new coll to digest dict

            all_dig[col] = list(col_embedded.keys())
            # Add data to embedded
            embedded.update(col_embedded)

        embedded.update(leaf_ci.embedded)

        new_ci = Commit(
            other_schema,
            leaf_ci.label,
            leaf_ci.start,
            leaf_ci.stop,
            all_dig,
            leaf_ci.length,
            closed=leaf_ci.closed,
            embedded=embedded,
        )
        return other_collection.changelog.commit(new_ci.encode(), parents=[phi])

    def refresh(self):
        self.changelog.refresh()

    def push(self, remote, shallow=False):
        return remote.pull(self, shallow=shallow)

    def pull(self, remote, shallow=False):
        """
        Pull remote series into self
        """
        assert isinstance(remote, Collection), "A Collection instance is required"

        local_digs = set(self.digests())
        if shallow:
            remote_digs = set(remote.digests(remote.changelog.leafs()))
        else:
            remote_digs = set(remote.digests())
        sync = lambda path: self.pod.write(path, remote.pod.read(path))
        with Pool() as pool:
            for dig in remote_digs:
                if dig in local_digs:
                    continue
                folder, filename = hashed_path(dig)
                path = folder / filename
                pool.submit(sync, path)

        self.changelog.pull(remote.changelog, shallow=shallow)

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
        self.batch = True  # TODO use a real batch instance but adapt
        # multi() to accept the list of heads as
        # parent (and batch will use first parent as
        # last_ci in flush)
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

    def squash(self, trim=None, max_chunk=settings.defrag_max_chunk):
        """
        Remove past revisions, collapse each series into one or few large
        frames. Returns newly created revisions.

        If `trim` is None (the default), all revisions older than
        twice the timeout (see `settings.timeout` in `utils.py`) are
        removed.  If set to False, the full history is kept. If set to
        a datetime, all the revisions older than the given value will
        be deleted, keeping the recent history.

        The `max_chunk` parameter defines a limit over which the
        method will rewrite a series. If a given series comprise a
        small number of chunk (aka less than `max_chunk`) it will be
        kept as is and no rewrite will be attempted.

        If `max_chunk` is less or equal to zero, no new revision is
        created.
        """
        warn(
            "Collection.squash will be deprecated, please use"
            "Collection.trim & Collection.defrag"
        )

        revs = self.defrag(max_chunk=max_chunk)
        if trim is not False:
            self.trim(trim)
        return revs

    def trim(self, before=None):
        """
        If `before` is None (the default), all revisions older than twice
        the timeout (see `settings.timeout` in `utils.py`) are
        removed.  If set to a datetime, all the revisions older than
        the given value will be deleted, keeping at least the last
        commit.
        """
        logger.info('Trim collection "%s"', self.label)
        if before is None:
            before = datetime.now() - timedelta(seconds=settings.timeout) * 2
        # Read existing revisions
        revs = self.changelog.log(before=before)
        if len(revs) <= 1:
            return 0
        self.changelog.pod.rm_many([r.path for r in revs[:-1]])
        self.changelog.refresh()
        return len(revs) - 1

    def defrag(self, max_chunk=settings.defrag_max_chunk):
        # Rewrite each series
        leaf = self.changelog.leaf()
        commit = leaf and leaf.commit(self)
        with self.multi() as batch:
            with Pool() as pool:
                for series in self:
                    pool.submit(self._defrag_series, series, commit, max_chunk)

        return batch.revs

    def _defrag_series(self, series, commit, max_chunk):
        logger.info('Defrag series "%s/%s"', self.label, series.label)
        # Re-write series. We use _find_defrag_start to fast-forward
        # in the series (we bet on the fact that most series are
        # append-only)
        start, closed = self._find_defrag_start(commit, series, max_chunk)
        prev_stop = None
        for frm in series.paginate(start=start, closed=closed):
            series.write(frm, start=prev_stop, closed="r" if prev_stop else "b")
            prev_stop = frm.stop()

    def _find_defrag_start(self, commit, series, max_chunk):
        """
        Find the first "small" segment , and return its start values.
        """
        assert max_chunk > 0, "Parameter 'max_chunk' must be bigger than 0"
        rows = list(commit.match(series.label))
        if len(rows) <= max_chunk:
            return rows[-1]["stop"], "RIGHT"

        # Define a minimal acceptable len
        total_len = sum(row["length"] for row in rows)
        threshold = min(settings.page_len, total_len / (max_chunk + 1))

        for row in rows[:-1]:
            # Stop at first small row
            if row["length"] < threshold:
                return row["start"], "BOTH"
        return rows[-1]["stop"], "RIGHT"

    def digests(self, revisions=None):
        if revisions is None:
            revisions = self.changelog.log()
        for rev in revisions:
            ci = rev.commit(self)
            digs = set(chain.from_iterable(ci.digest.values()))
            # return only digest not already embedded in the commit
            digs = digs - set(ci.embedded)
            yield from digs

    @contextmanager
    def multi(self, root=None):
        with self._batch_lock:
            b = Batch(self, root)
            self.batch = b
            yield b
            b.flush()
            self.batch = None
