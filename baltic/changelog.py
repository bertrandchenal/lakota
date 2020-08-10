from collections import defaultdict
from random import random
from time import sleep

import numpy

from .schema import Schema
from .utils import hexdigest, hextime, tail

phi = "0" * 40


# TODO implement a proper revision object (with parent, child, path and
# payload fields)


class Changelog:

    """
    Build a tree over a pod to provide concurrent commits
    """

    schema = Schema(["revision:O|msgpack2|zstd"])

    def __init__(self, pod):
        self.pod = pod

    def commit(self, revision, _jitter=False, force_parent=None):
        # Find parent & write revisions
        if force_parent:
            parent = force_parent
        else:
            parent = self.leaf()
            if parent is None:
                parent = phi
            else:
                parent = parent.split(".", 1)[1]

        # Debug helper
        if _jitter:
            sleep(random())

        arr = numpy.array([revision])
        data = self.schema["revision"].encode(arr)
        key = hexdigest(data)
        if parent.endswith(key):
            # Prevent double writes
            rev, _ = parent.split("-")
            return rev

        # Construct new filename and save content
        rev = hextime()
        filename = f"{parent}.{rev}-{key}"
        self.pod.write(filename, data)
        return rev

    def __iter__(self):
        yield from self.pod.ls(raise_on_missing=False)

    def log(self):
        """
        Create a parent:[child] dict of all the revisions
        """
        log = defaultdict(list)
        for name in sorted(self):
            parent, child = name.split(".")
            if parent == child:
                continue
            log[parent].append(child)
        return log

    def leaf(self):
        res = tail(self.walk(), 1)
        if not res:
            return None
        path, _ = res[0]
        return path

    def walk(self, parent=phi):
        """
        Depth-first traversal of the tree
        """
        # [XXX] re-executing a full walk all the time is costly, we
        # could cache the last result and (based on log content)
        # bootstrap the loop

        # see DFS in https://stackoverflow.com/a/5278667
        log = self.log()
        revs = [(parent, c) for c in reversed(log[parent])]
        while revs:
            rev = revs.pop()
            parent, child = rev
            # Yield from first child
            path = f"{parent}.{child}"
            rev_arr = self.extract(path)
            for item in rev_arr:
                yield path, item
            # Append children
            revs.extend((child, c) for c in reversed(log[child]))

    def extract(self, path):
        try:
            revision = self.pod.read(path)
        except FileNotFoundError:
            return
        return self.schema["revision"].decode(revision)

    def pull(self, remote):
        new_revs = []
        local_revs = set(self)
        for rev in remote:
            if rev in local_revs:
                continue
            new_revs.append(rev)
            payload = remote.pod.read(rev)
            self.pod.write(rev, payload)
        return new_revs

    def pack(self):
        """
        Combine the current list of revisions into one array of revision
        """
        items = list(self.walk())
        paths, revisions = zip(*items)
        if len(revisions) == 1:
            return

        parent = phi
        arr = numpy.array(revisions)
        data = self.schema["revision"].encode(arr)
        key = hexdigest(data, parent.encode())
        filename = ".".join((parent, key))
        self.pod.write(filename, data)

        # Clean old revisions
        for path in set(paths):
            self.pod.rm(path)
