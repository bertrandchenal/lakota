from collections import defaultdict
from random import random
from time import sleep

from .utils import hexdigest, hexhash_len, hextime, tail

zero_hextime = "0" * 11
zero_hash = "0" * hexhash_len
phi = f"{zero_hextime}-{zero_hash}"


class Changelog:

    """
    Build a tree over a pod to provide concurrent revisions
    """

    def __init__(self, pod):
        self.pod = pod
        self._walk_cache = None

    def commit(self, payload, parent=None, _jitter=False):
        assert isinstance(payload, bytes)

        # Find parent & write revisions
        if parent is None:
            last_revision = self.leaf()
            if last_revision is None:
                parent = phi
            else:
                parent = last_revision.child

        # Debug helper
        if _jitter:
            sleep(random())

        # Create array and encode it
        key = hexdigest(payload)
        if parent is not phi:
            parent_key = parent.split("-", 1)[1]
            if parent_key == key:
                # Catch double writes
                return

        # Construct new filename and save content
        child = hextime() + "-" + key
        revision = Revision(self, parent, child)
        self.pod.write(revision.path, payload)
        self.refresh()
        return revision

    def refresh(self):
        self._walk_cache = None

    def __iter__(self):
        yield from self.pod.ls(missing_ok=True)

    def leaf(self, before=None):
        revisions = tail(self.log(before=before), 1)
        if not revisions:
            return None
        return revisions[0]

    def log(self, before=None, from_parent=phi):
        """
        Re-Create a list of all the active revisions
        """
        # Extract parent->children relations
        revisions = defaultdict(list)
        for name in sorted(self):
            parent, child = name.split(".")
            if parent == child:
                continue
            revisions[parent].append(Revision(self, parent, child))

        # Depth first traversal of the tree(see
        # https://stackoverflow.com/a/5278667)
        queue = list(reversed(revisions[from_parent]))
        while queue:
            item = queue.pop()
            # Append children
            queue.extend(reversed(revisions[item.child]))

            # Yield
            if before is not None and item.epoch >= before:
                continue
            yield item

        # TODO detect dangling roots

    # def walk(self, fltr=None):
    #     """
    #     Iterator on the list of commits
    #     """
    #     if not self._walk_cache:
    #         revs = []
    #         # TODO build a frame (index ?) instead of a list of objects
    #         for commit in self.log():
    #             revs.extend(self.extract(commit))
    #         self._walk_cache = revs

    #     if not fltr:
    #         yield from self._walk_cache
    #         return

    #     for rev in filter(fltr, self._walk_cache):
    #         yield rev

    # def extract(self, commit):
    #     try:
    #         data = self.pod.read(commit.path)
    #     except FileNotFoundError:
    #         return
    #     for payload in self.schema["revision"].decode(data):
    #         yield Revision(commit=commit, payload=payload)

    def pull(self, remote):
        new_paths = []
        local_digests = set(Revision.from_path(self, p).digests for p in self)
        for remote_path in remote:
            remote_ci = Revision.from_path(remote, remote_path)
            if remote_ci.digests in local_digests:
                continue
            new_paths.append(remote_path)
            payload = remote.pod.read(remote_path)
            self.pod.write(remote_path, payload)
        self.refresh()
        return new_paths

    def pack(self, fltr=None):
        """
        Combine the current list of revisions into one array of revision.
        The filter function `fltr` allows to keep only some items (and
        so remove the others).
        """

        revs = list(self.walk(fltr=fltr))
        if not revs:
            return
        if not fltr:
            # we can skip packing if there is only one previous revision
            # or if previous revision is alreay a packing one
            if len(set(r.path for r in revs)) == 1:
                return

        revision = self.revision([r.payload for r in revs], force_parent=phi)
        # Clean other revisions
        self.pod.clear(revision.path)
        return revision


class Revision:
    def __init__(self, changelog, parent, child):
        self.changelog = changelog
        self.parent = parent
        self.child = child

    @classmethod
    def from_path(cls, changelog, path):
        parent, child = path.split(".")
        return Revision(changelog, parent, child)

    @property
    def digests(self):
        items = (self.parent, self.child)
        return tuple(i.split("-")[1] for i in items)

    @property
    def path(self):
        return f"{self.parent}.{self.child}"

    @property
    def epoch(self):
        return self.child.split("-", 1)[0]

    def __repr__(self):
        return f"<Revision {self.path}>"

    def read(self):
        payload = self.changelog.pod.read(self.path)
        key = hexdigest(payload)
        _, child_digest = self.digests
        assert key == child_digest, "Corrupted file!"
        return payload
