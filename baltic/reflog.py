from collections import defaultdict

from .store import get_store
from .utils import tail


phi = '0'*40


class RefLog:

    '''
    Build a tree over a store to provide concurrent commits
    '''

    def __init__(self, root):
        self.store = get_store(root)

    def commit(self, key, content, parent=None):
        if parent is None:
            # Find parent
            parent = self.find_leaf()

        # Create parent.child
        filename = '.'.join(parent, key)
        self.store(filename, content)

    def log(self):
        '''
        Create a parent:[child] dict of all the revisions
        '''
        listing = self.store.listdir()
        log = defaultdict(list)
        for name in listing:
            parent, child = name.split('.')
            log[parent].append(child)
        return log

    def find_leaf(self):
        leaf, = tail(self.walk(), 1)
        return leaf

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
