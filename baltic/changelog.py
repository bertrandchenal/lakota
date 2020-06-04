from time import sleep
from random import random
from collections import defaultdict

import zarr

from .utils import tail, head


phi = '0'*40


# TODO: Use multi-col segment instead of a unique col with a large
# string in it.

class Changelog:

    '''
    Build a tree over a zarr group to provide concurrent commits
    '''

    def __init__(self, group):
        self.group = group

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
        arr =  zarr.array(items, dtype=str)
        key = arr.hexdigest()
        filename = '.'.join((parent, key))

        # XXX add .pkg ext that will pack a list of revs in one file
        zarr.copy(arr, self.group, filename, if_exists='skip')
        return filename

    def __iter__(self):
        return iter(self.group)

    def log(self):
        '''
        Create a parent:[child] dict of all the revisions
        '''
        log = defaultdict(list)
        for name in self:
            parent, child = name.split('.')
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
        children = log.get(parent, [])
        for child in children:
            path = '{}.{}'.format(parent, child)
            yield path
            yield from self._walk(log, child)

    # def read(self, revision):
    #     return self.group[revision]

    def read(self, parent=phi):
        for rev in self.walk(parent=parent):
            yield from self.group[rev]

    def pack(self):
        '''
        Combine the current list of revisions into one array of revision
        '''
        items = []
        revisions = []
        for rev in self.walk():
            revisions.append(rev)
            items.extend(self.group[rev])
        if len(revisions) == 1:
            return
        self.commit(items, parent=phi)

        # Clean old revisions
        for rev in revisions:
            del self.group[rev]

