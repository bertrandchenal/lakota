import zarr

# XXX maybe we should have different store for repo and segment
class Store:
    '''
    Abstract IO operations
    '''

    def __init__(self, root):
        self.root = root

    def group(self, path):
        return zarr.group(self.root / path)

    def get(self, key):
        path = self.root / key
        with path.open('rb') as fd:
            return fd.read()

    def set(self, key, value):
        path = self.root / key
        with path.open('wb') as fd:
            fd.write(value)

    def listdir(self, path='.'):
        return (self.root / path).iterdir()


def get_store(path):
    return Store(path)
