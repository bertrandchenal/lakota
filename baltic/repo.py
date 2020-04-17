from pathlib import Path

from .reflog import RefLog, phi
from .store import get_store
from .utils import default_hash
from .segment import Segment


class Repo:
    '''
    Combine a store and a reflog to provide a versioned and concurrent
    management of timeseries.
    '''

    def __init__(self, root):
        self.root = Path(root)
        self.reflog = RefLog(self.root / 'refs')
        self.sgm_store = get_store(self.root / 'segments')
        self._schema = None

    @property
    def schema(self):
        if self._schema:
            return self._schema
        # Find root
        res = self.reflog.head(1)
        if not res:
            return res
        self._schema, = res
        return self._schema

    def init(self, schema):
        content = schema.dumps()
        key = default_hash(content).hexdigest()
        self.reflog.commit(key, content, phi)

    def read(self, start, stop):
        '''
        Read underlying array between start and stop
        '''
        pass

    def write(self, df, idx_start=None, idx_end=None):
        # TODO user idx_range if given
        # TODO verify schema
        sgm = Segment.from_df(self.schema, df)
        lines = []
        for name, digest in sgm.hexdigests():
            prefix, suffix = digest[:2], digest[2:]
            sgm.copy(name, self.sgm_store.group(prefix), suffix)
            lines.append(' '.join(name, digest))
        content = '\n'.join(lines)
        key = default_hash(content).hexdigest()
        self.reflog.commit(key, content)

    def squash(self, from_revision=None, to_revision=None):
        '''
        Collapse all revision between the two
        '''
        from_revision = from_revision or phi

        # TODO
