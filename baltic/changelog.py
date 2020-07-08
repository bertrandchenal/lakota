from collections import defaultdict
from random import random
from time import sleep, time

import numpy

from .schema import Schema
from .utils import hexdigest, tail

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

    def commit(self, content, parent=None, _jitter=False):
        # Debug helper
        if _jitter:
            sleep(random())

        # Find parent
        if parent is None:
            parent = self.leaf()
            if parent is None:
                parent = phi
            else:
                parent = parent.split(".", 1)[1]

        revision = {'timestamp': time(), 'content': content}
        return self.write([revision], parent=parent)

    def write(self, revisions, parent):

        # Create parent.child
        arr = numpy.array(revisions)
        data = self.schema.encode("revision", arr)
        key = hexdigest(data, parent.encode())
        filename = ".".join((parent, key))
        nb_bytes = self.pod.write(filename, data)
        if nb_bytes is None:
            return
        return filename

    def __iter__(self):
        yield from self.pod.ls(raise_on_missing=False)

    def log(self):
        """
        Create a parent:[child] dict of all the revisions
        """
        log = defaultdict(list)
        for name in self:
            parent, child = name.split(".")
            if parent == child:
                continue
            log[parent].append(child)
        return log

    def leaf(self):
        res = tail(self._walk(), 1)
        if not res:
            return None
        return res[0]['path']

    def walk(self, parent=phi):
        """
        Depth-first traversal of the tree
        """
        for revision in self._walk(parent=parent):
            yield revision['content']

    def _walk(self, parent=phi):
        """
        Low-level version of walk
        """
        log = self.log()

        revs = [(parent, child) for child in log[parent]]
        while revs:
            # Yield first all siblings
            yield from self.extract((f"{p}.{c}" for p, c in revs))
            # Build next level
            revs = [(c, grand_child) for _, c in revs for grand_child in log[c]]

    def extract(self, paths):
        for p in paths:
            content = self.pod.read(p)
            revisions = self.schema.decode("revision", content)
            for rev in revisions:
                rev['path'] = p
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
        self.write(revisions, parent=phi)

        # Clean old revisions
        for rev in revisions:
            self.pod.rm(rev['path'])
