class Store:
    '''
    Abstract IO operations
    '''

    def __init__(self, root):
        self.root = root

    def save(self, key, value):
        path = self.root / key
        with path.open('wb') as fd:
            fd.write(value)

    def listdir(self, path='.'):
        return (self.root / path).iterdir()

    def read(self, key):
        pass


def get_store(path):
    return Store(path)
