from pathlib import Path

from .reflog import RefLog, phi
from .store import get_store
from .utils import take, default_hash


class Repo:
    '''
    Combine a store and a reflog to provide a versioned and concurrent
    management of timeseries.
    '''

    def __init__(self, root):
        self.root = Path(root)
        self.reflog = RefLog(self.root / 'refs')
        self.segments = get_store(self.root / 'segments')

    def init(self, schema):
        content = schema.dumps()
        key = default_hash(content).hexdigest()
        self.reflog.commit(key, content, phi)

    def read(self, filters):
        pass

    def write(self):
        pass

    def sqash(self, from_revision=None, to_revision=None):
        '''
        Collapse all revision between the two
        '''
        from_revision = from_revision or phi

        # TODO
