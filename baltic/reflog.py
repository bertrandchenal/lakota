from collections import defaultdict

from .utils import tail, head


phi = '0'*40


class RefLog:

    '''
    Build a tree over a store to provide concurrent commits
    '''

    def __init__(self, store):
        self.store = store

    def commit(self, key, content, parent=None):
        if parent is None:
            # Find parent
            parent = self.find_leaf()
            if parent is None:
                parent = phi
            else:
                parent = parent.split('.', 1)[1]

        # Create parent.child
        filename = '.'.join((parent, key))
        # XXX add and extension (.sch or .sgm)
        self.store[filename] = content

    def read(self, revision):
        return self.store[revision]

    def log(self):
        '''
        Create a parent:[child] dict of all the revisions
        '''
        listing = list(self.store)
        log = defaultdict(list)
        for name in listing:
            parent, child = name.split('.')
            log[parent].append(child)
        return log

    def find_leaf(self):
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
