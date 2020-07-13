from collections import defaultdict
from random import random
from time import sleep, time

import numpy

from .schema import Schema
from .utils import hexdigest, tail, timedigest

phi = "0" * 40


# TODO: Use multi-col segment instead of a unique col with a large
# string in it.


class Changelog:

    """
    Build a tree over a pod to provide concurrent commits
    """

    schema = Schema(["revision:O|json|zstd"])

    def __init__(self, pod):
        self.pod = pod

    def commit(self, content, _jitter=False, force_parent=None):
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

        revision = {"timestamp": time(), "content": content}
        arr = numpy.array([revision])
        data = self.schema.encode("revision", arr)
        key = timedigest(data, parent.encode())

        filename = ".".join((parent, key))
        self.pod.write(filename, data)
        return filename

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
        res = tail(self._walk(), 1)
        if not res:
            return None
        return res[0]["path"]

    def walk(self, parent=phi):
        """
        Depth-first traversal of the tree
        """
        for revision in self._walk(parent=parent):
            yield revision["content"]

    def _walk(self, parent=phi):
        """
        Low-level version of walk
        """
        # see DFS in https://stackoverflow.com/a/5278667
        log = self.log()
        revs = [(parent, c) for c in reversed(log[parent])]
        while revs:
            rev = revs.pop()
            parent, child = rev
            # Yield from first child
            yield from self.extract(f"{parent}.{child}")
            # Append children
            revs.extend((child, c) for c in reversed(log[child]))

    def extract(self, path):
        try:
            content = self.pod.read(path)
        except FileNotFoundError:
            return
        revisions = self.schema.decode("revision", content)
        for rev in revisions:
            rev["path"] = path
            yield rev

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
        revisions = list(self._walk())
        if len(revisions) == 1:
            return

        parent = phi
        arr = numpy.array(revisions)
        data = self.schema.encode("revision", arr)
        key = hexdigest(data, parent.encode())
        filename = ".".join((parent, key))
        self.pod.write(filename, data)

        # Clean old revisions
        for rev in revisions:
            self.pod.rm(rev["path"])
