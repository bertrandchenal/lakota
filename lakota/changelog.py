from collections import defaultdict
from random import random
from time import sleep

import numpy

from .frame import ShallowSegment
from .schema import Schema
from .utils import hexdigest, hextime, tail

zero_hex = "0" * 11
zero_hash = "0" * 40
phi = f"{zero_hex}-{zero_hash}"


class Changelog:

    """
    Build a tree over a pod to provide concurrent commits
    """

    schema = Schema(["revision O*"])  # |msgpack2|zstd

    def __init__(self, pod):
        self.pod = pod
        self._walk_cache = None

    def commit(self, payload, key=None, force_parent=None, _jitter=False):
        # Find parent & write revisions
        if force_parent:
            parent = force_parent
        else:
            last_commit = self.leaf()
            if last_commit is None:
                parent = phi
            else:
                parent = last_commit.child

        # Debug helper
        if _jitter:
            sleep(random())

        # Create array and encode it
        if not isinstance(payload, (list, tuple)):
            payload = [payload]
        arr = numpy.array(payload)
        data = self.schema["revision"].encode(arr)
        if key is None:
            key = hexdigest(data)
        if parent is not phi:
            parent_key = parent.split("-", 1)[1]
            if parent_key == key:
                # Catch double writes
                return

        # Construct new filename and save content
        child = hextime() + "-" + key
        commit = Commit(parent, child)
        self.pod.write(commit.path, data)
        self.refresh()
        return commit

    def refresh(self):
        self._walk_cache = None

    def __iter__(self):
        yield from self.pod.ls(missing_ok=True)

    def leaf(self):
        commits = tail(self.log(), 1)
        if not commits:
            return None
        return commits[0]

    def log(self, from_parent=phi):
        """
        Re-Create a list of all the active commits
        """
        # Extract parent->children relations
        commits = defaultdict(list)
        for name in sorted(self):
            parent, child = name.split(".")
            if parent == child:
                continue
            commits[parent].append(Commit(parent, child))

        # Depth first traversal of the tree(see
        # https://stackoverflow.com/a/5278667)
        queue = list(reversed(commits[from_parent]))
        while queue:
            item = queue.pop()
            yield item
            # Append children
            queue.extend(reversed(commits[item.child]))

        # TODO detect dangling roots

    def walk(self, fltr=None):
        """
        Iterator on the list of commits
        """
        if not self._walk_cache:
            revs = []
            # TODO build a frame (index ?) instead of a list of objects
            for commit in self.log():
                revs.extend(self.extract(commit))
            self._walk_cache = revs

        if not fltr:
            yield from self._walk_cache
            return

        for rev in filter(fltr, self._walk_cache):
            yield rev

    def extract(self, commit):
        try:
            data = self.pod.read(commit.path)
        except FileNotFoundError:
            return
        for payload in self.schema["revision"].decode(data):
            yield Revision(commit=commit, payload=payload)

    def pull(self, remote):
        new_paths = []
        local_digests = set(Commit.from_path(p).digests for p in self)
        for remote_path in remote:
            remote_ci = Commit.from_path(remote_path)
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
            # we can skip packing if there is only one previous commit
            # or if previous commit is alreay a packing one
            if len(set(r.path for r in revs)) == 1:
                return

        commit = self.commit([r.payload for r in revs], force_parent=phi)
        # Clean other commits
        self.pod.clear(commit.path)
        return commit


class Revision:
    def __init__(self, commit, payload=None):
        self.payload = payload or {}
        self.commit = commit

    def __getitem__(self, name):
        return self.payload[name]

    def __setitem__(self, name, value):
        self.payload[name] = value

    def __repr__(self):
        return f"<revision {self.payload}>"

    def get(self, name, default=None):
        return self.payload.get(name, default)

    @property
    def path(self):
        return self.commit.path

    def segment(self, series):
        return ShallowSegment(
            series.schema,
            series.pod,
            self["digests"],
            start=self["start"],
            stop=self["stop"],
            length=self["len"],
        )


class Commit:
    def __init__(self, parent, child):
        self.parent = parent
        self.child = child

    @classmethod
    def from_path(cls, path):
        parent, child = path.split(".")
        return Commit(parent, child)

    @property
    def digests(self):
        items = (self.parent, self.child)
        return tuple(i.split("-")[1] for i in items)

    @property
    def path(self):
        return f"{self.parent}.{self.child}"

    def __repr__(self):
        return f"<Commit {self.path}>"
