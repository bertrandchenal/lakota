from .changelog import phi
from .commit import Commit

# TODO batch should first do collection.repo.pod.read('arena') to know
# where to write. Or instanciate an arena object that abstract those
# concepts.  it should also raise an exception if elapsed time since
# the first write gets too big.

# NOTE that changelog is not affected, because it creates data that is
# not impacted by gc

__all__ = ["Batch"]


class Batch:
    def __init__(self, collection, root=False):
        self.collection = collection
        self._ci_info = []
        self.revs = []
        self.root = root

    def append(self, label, start, stop, all_dig, frame_len, closed, embedded):
        self._ci_info.append((label, start, stop, all_dig, frame_len, closed, embedded))

    def extend(self, *other_batches):
        for b in other_batches:
            self._ci_info.extend(b._ci_info)

    def flush(self):
        # TODO abort flush if timeout is reached !

        if len(self._ci_info) == 0:
            return

        changelog = self.collection.changelog
        leaf_rev = None if self.root else changelog.leaf()
        all_ci_info = iter(self._ci_info)

        # Combine with last commit
        if leaf_rev:
            last_ci = leaf_rev.commit(self.collection)
        else:
            label, start, stop, all_dig, length, closed, embedded = next(all_ci_info)
            last_ci = Commit.one(
                self.collection.schema,
                label,
                start,
                stop,
                all_dig,
                length,
                closed=closed,
                embedded=embedded,
            )
        for label, start, stop, all_dig, length, closed, embedded in all_ci_info:
            last_ci = last_ci.update(
                label, start, stop, all_dig, length, closed=closed, embedded=embedded
            )

        # Save it
        payload = last_ci.encode()
        parent = leaf_rev.child if leaf_rev else phi
        self.revs = self.collection.changelog.commit(payload, parents=[parent])
