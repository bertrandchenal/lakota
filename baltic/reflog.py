from time import sleep
from random import random
from collections import defaultdict

from .utils import tail, head, digest


phi = '0'*40


class RefLog:

    '''
    Build a tree over a store to provide concurrent commits
    '''

    def __init__(self, store):
        self.store = store

    def commit(self, content, parent=None, _jitter=False):
        key = digest(content)
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
        filename = '.'.join((parent, key))
        # XXX add an extension (.sch, .sgm, .pkg)
        self.store[filename] = content
        return filename

    def read(self, revision):
        return self.store[revision]

    def __iter__(self):
        return iter(self.store)

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

    def walk(self):
        '''
        Depth-first traversal of the three
        '''
        log = self.log()
        parent = phi
        while True:
            children = log.get(parent)
            if not children:
                break
            first = sorted(children)[0]
            yield '{}.{}'.format(parent, first)
            parent = first
