from collections import defaultdict
from random import random
from time import sleep

import numpy

from .frame import ShallowSegment
from .schema import Schema
from .utils import hexdigest, hextime, tail

phi = "0" * 40


class Changelog:

    """
    Build a tree over a pod to provide concurrent commits
    """

    schema = Schema(["revision:O|msgpack2|zstd"])

    def __init__(self, pod):
        self.pod = pod

    def commit(self, payload, _jitter=False, force_parent=None):
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
            payload = [
                payload
            ]  # XXX wrap payload like '{epoch: .., payload: payload}' this will make digest more stable
        arr = numpy.array(payload)
        data = self.schema["revision"].encode(arr)
        key = hexdigest(data)

        # Construct new filename and save content
        child = hextime() + "-" + key
        commit = Commit(parent, child)
        self.pod.write(commit.path, data)
        return commit

    def __iter__(self):
        yield from self.pod.ls(raise_on_missing=False)

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

    def walk(self, parent=phi):
        """
        Iterator on the list of commits
        """
        # [XXX] re-executing a full walk all the time is costly, we
        # could cache the last result and (based on log content)
        # bootstrap the loop
        for commit in self.log():
            rev_arr = self.extract(commit.path)
            for payload in rev_arr:
                yield Revision(commit=commit, payload=payload)

    def extract(self, path):
        try:
            data = self.pod.read(path)
        except FileNotFoundError:
            return
        return self.schema["revision"].decode(data)

    def pull(self, remote):
        new_paths = []
        local_paths = set(self)
        for remote_path in remote:
            if remote_path in local_paths:
                continue
            new_paths.append(remote_path)
            payload = remote.pod.read(remote_path)
            self.pod.write(remote_path, payload)
        return new_paths

    def pack(self):
        """
        Combine the current list of revisions into one array of revision
        """

        # TODO: allow to only pack commit that are old enough (so not
        # the most recent ones) to not disturb concurrent writes. Also
        # prevent dangling commits (when the parent has already been
        # packed)
        revs = list(self.walk())
        if len(revs) == 1:
            return
        self.commit([r.payload for r in revs], force_parent=phi)
        # Clean old revisions
        for path in set(r.path for r in revs):
            self.pod.rm(path)


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

    @property
    def path(self):
        return self.commit.path

    def segment(self, series):
        return ShallowSegment(
            series.schema,
            series.segment_pod,
            self["digests"],
            start=self["start"],
            stop=self["stop"],
            length=self["len"],
        )


class Commit:
    def __init__(self, parent, child):
        self.parent = parent
        self.child = child

    @property
    def path(self):
        return self.parent + "." + self.child

    def __repr__(self):
        return f"<Commit {self.path}>"
