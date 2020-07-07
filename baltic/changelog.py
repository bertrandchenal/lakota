from collections import defaultdict
from random import random
from time import sleep

import numpy

from .schema import Schema
from .utils import head, hexdigest, tail

phi = "0" * 40


# TODO: Use multi-col segment instead of a unique col with a large
# string in it.


class Changelog:

    """
    Build a tree over a zarr group to provide concurrent commits
    """

    schema = Schema(["revision:O|json|zstd"])

    def __init__(self, pod):
        self.pod = pod

    def commit(self, revisions, parent=None, _jitter=False):
        # Find parent
        if parent is None:
            # Find parent
            parent = self.leaf()
            if parent is None:
                parent = phi
            else:
                parent = parent.split(".", 1)[1]

        # Debug helper
        if _jitter:
            sleep(random())

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
        res = tail(self.walk(), 1)
        if not res:
            return None
        return res[0]

    def head(self, count):
        return head(self.walk(), count)

    def walk(self, parent=phi):
        """
        Depth-first traversal of the three
        """
        log = self.log()
        yield from self._walk(log, parent=parent)

    def _walk(self, log, parent=phi):
        """
        Depth-first traversal of the changelog tree
        """
        children = log.get(parent, [])
        for child in children:
            path = f"{parent}.{child}"
            yield path
            yield from self._walk(log, child)

    def extract(self, revs=None):
        if not revs:
            revs = self.walk()
        # Read is not the correct name
        # read should do open / read / decode of a given rev
        for rev in revs:
            content = self.pod.read(rev)
            revisions = self.schema.decode("revision", content)
            yield from revisions

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
        keys = list(self.walk())
        if len(keys) == 1:
            return
        revisions = list(self.extract(keys))
        self.commit(revisions, parent=phi)

        # Clean old revisions
        for key in keys:
            self.pod.rm(key)
