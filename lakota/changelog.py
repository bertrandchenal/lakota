from collections import defaultdict, namedtuple
from itertools import chain
from random import random
from time import sleep

from .commit import Commit
from .utils import hexdigest, hexhash_len, hextime, tail

zero_hextime = "0" * 11
zero_hash = "0" * hexhash_len
phi = f"{zero_hextime}-{zero_hash}"

__all__ = ["Changelog", "Revision"]


class Changelog:

    """
    Build a tree over a pod to provide concurrent revisions
    """

    def __init__(self, pod):
        self.pod = pod
        self._log_cache = None

    def commit(self, payload, parents=None, _jitter=False):
        assert isinstance(payload, bytes)

        # Find parent & write revisions
        if not parents:
            last_revision = self.leaf()
            if last_revision is None:
                parents = [phi]
            else:
                parents = [last_revision.child]

        # Debug helper
        if _jitter:
            sleep(random())

        # Compute new key
        key = hexdigest(payload)

        # Create one commit per parent
        revs = []
        for parent in parents:
            if parent is not phi:
                parent_key = parent.split("-", 1)[1]
                if parent_key == key:
                    # Catch double writes
                    continue

            # Construct new filename and save content
            child = hextime() + "-" + key
            revision = Revision(self, parent, child)
            self.pod.write(revision.path, payload)
            revs.append(revision)

        self.refresh()
        return revs

    def refresh(self):
        self._log_cache = None

    def __iter__(self):
        yield from self.pod.ls(missing_ok=True)

    def leaf(self, before=None):
        revisions = tail(self.log(before=before), 1)
        if not revisions:
            return None
        return revisions[0]

    def leafs(self):
        return [rev for rev in self.log() if rev.is_leaf]

    def log(self, before=None):
        """
        Create a list of all the active revisions
        """
        if before is not None:
            return self._log(before)
        if self._log_cache is None:
            self._log_cache = list(self._log())
        return self._log_cache

    def _log(self, before=None):
        # Extract parent->children relations
        revisions = defaultdict(list)
        all_children = set()
        for name in sorted(self):
            parent, child = name.split(".")
            if parent == child:
                continue
            all_children.add(child)
            revisions[parent].append(Revision(self, parent, child))

        # `revision` is sorted low to high (because filled based on
        # `sorted(self)`, so `queue` is sorted too (high to low). So
        # the last revision to be yield is the last child of the
        # newest branch (aka oldest parent)
        parent_revs = [r for r in revisions if r not in all_children]
        first_gen = list(chain.from_iterable(revisions[p] for p in parent_revs))
        queue = list(reversed(first_gen))

        # Depth first traversal of the tree(see
        # https://stackoverflow.com/a/5278667)
        while queue:
            rev = queue.pop()
            # Append children
            children = revisions[rev.child]
            rev.is_leaf = not children
            queue.extend(reversed(children))

            # Yield
            if before is not None and rev.epoch >= before:
                break
            yield rev

    def pull(self, remote):
        new_paths = []
        local_digests = set(Revision.from_path(self, p).digests for p in self)
        for remote_path in remote:
            remote_rev = Revision.from_path(remote, remote_path)
            if remote_rev.digests in local_digests:
                continue
            new_paths.append(remote_path)
            payload = remote.pod.read(remote_path)
            self.pod.write(remote_path, payload)
        self.refresh()
        return new_paths


RevDigest = namedtuple("RevDigest", ["parent", "child"])


class Revision:
    def __init__(self, changelog, parent, child):
        self.changelog = changelog
        self.parent = parent
        self.child = child
        self.is_leaf = False
        self._payload = None

    @classmethod
    def from_path(cls, changelog, path):
        parent, child = path.split(".")
        return Revision(changelog, parent, child)

    @property
    def digests(self):
        items = (self.parent, self.child)
        return RevDigest(*(i.split("-")[1] for i in items))

    @property
    def path(self):
        return f"{self.parent}.{self.child}"

    @property
    def epoch(self):
        return self.child.split("-", 1)[0]

    def __repr__(self):
        return f"<Revision {self.path} {'*' if self.is_leaf else ''}>"

    def read(self):
        if self._payload is not None:
            return self._payload
        for i in range(1, 5):
            payload = self.changelog.pod.read(self.path)
            key = hexdigest(payload)
            _, child_digest = self.digests
            # Incorrect checksum is usualy because file is being written concurrently
            if key == child_digest:
                self._payload = payload
                return payload
            else:
                sleep(i / 10)
        raise RuntimeError("Unable to read {self.path}")

    def commit(self, collection):
        """
        Instanciate commit based on self payload and series schema
        """
        payload = self.read()
        return Commit.decode(collection.schema, payload)
