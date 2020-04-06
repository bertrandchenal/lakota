from collections import defaultdict
from os import listdir
from os.path import join

phi = '0'*40


class RefLog:
    '''
    RefLog will save parent-child relations of keys (usually sha1
    cheksum), forming together a log of changesets.
    '''

    def __init__(self, root):
        self.root = root

    def save(self, name, data):
        # Find parent
        parent = self.find_leaf()

        # Create parent.child
        filename = '{}.{}'.format(parent, name)
        path = join(self.root, filename)
        with open(path, 'wb') as fd:
            fd.write(data)

    def log(self):
        listing = listdir(self.root)
        log = defaultdict(list)
        for name in listing:
            parent, child = name.split('.')
            log[parent].append(child)
        return log

    def find_leaf(self):
        log = self.log()
        parent = phi
        while True:
            children = log.get(parent)
            if not children:
                return parent
            parent = sorted(children)[0]

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
