from collections import defaultdict
from hashlib import sha1
from pathlib import Path
from random import random
from time import sleep

from numcodecs import VLenUTF8
import numpy

from .utils import tail, head


phi = '0'*40


# TODO: Use multi-col segment instead of a unique col with a large
# string in it.

# IDEA: model each rev of the changelog as a segment column (and use
# segment to do encode/decode and hexdigest)

class Changelog:

    '''
    Build a tree over a zarr group to provide concurrent commits
    '''

    def __init__(self, pod):
        self.pod = pod

    def commit(self, items, parent=None, _jitter=False):
        # Find parent
        if parent is None:
            # Find parent
            parent = self.leaf()
            if parent is None:
                parent = phi
            else:
                parent = parent.split('.', 1)[1]

        # Debug helper
        if _jitter:
            sleep(random())

        # Create parent.child
        arr =  numpy.array(items) #, dtype=str FIXME
        data = VLenUTF8().encode(arr)
        key = sha1(arr).hexdigest()
        filename = '.'.join((parent, key))

        self.pod.write(filename, data)
        return filename

    def __iter__(self):
        yield from self.pod.ls(raise_on_missing=False)

    def log(self):
        '''
        Create a parent:[child] dict of all the revisions
        '''
        log = defaultdict(list)
        for name in self:
            parent, child = name.split('.')
            parent = Path(parent).stem # FIXME should be handle by POD object
            if parent == child:
                # FIXME Maybe not create parent.child in the first place!
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
        '''
        Depth-first traversal of the three
        '''
        log = self.log()
        yield from self._walk(log, parent=parent)

    def _walk(self, log, parent=phi):
        '''
        Depth-first traversal of the changelog tree
        '''
        children = log.get(parent, [])
        for child in children:
            path = '{}.{}'.format(parent, child)
            yield path
            yield from self._walk(log, child)

    def read(self, parent=phi):
        # Read is not the correct name
        # read should do open / read / decode of a given rev
        codec = VLenUTF8()
        for rev in self.walk(parent=parent):
            content = self.pod.read(rev)
            yield from codec.decode(content)

    def pack(self):
        '''
        Combine the current list of revisions into one array of revision
        '''
        items = []
        revisions = list(self.walk())
        if len(revisions) == 1:
            return
        items = list(self.read())
        self.commit(items, parent=phi)

        # Clean old revisions
        for rev in revisions:
            self.pod.rm(rev)
