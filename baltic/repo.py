from pathlib import Path
import json

import zarr

from .reflog import RefLog
from .segment import Segment
from .schema import Schema
from .utils import skip

class Repo:
    '''
    Combine a store and a reflog to provide a versioned and concurrent
    management of timeseries.
    '''

    def __init__(self, path=None):
        if path:
            path = Path(path)
            self.reflog_store = zarr.DirectoryStore(path / 'refs')
            self.sgm_store = zarr.DirectoryStore(path / 'segments')
        else:
            self.reflog_store = zarr.MemoryStore()
            self.sgm_store = zarr.MemoryStore()

        self.reflog = RefLog(self.reflog_store)
        self.sgm_grp = zarr.group(store=self.sgm_store)
        self._schema = None

    @property
    def schema(self):
        if self._schema:
            return self._schema
        # Find root
        res = self.reflog.head(1)
        if not res:
            return res
        content = self.reflog.store.get(res[0])
        schema = Schema(**json.loads(content))
        self._schema = schema
        return self._schema

    def init(self, schema):
        content = schema.dumps()
        self.reflog.commit(content.encode())

    def read(self, start=None, stop=None):
        '''
        Read underlying array between start and stop
        '''
        # Follow revisions backwards
        revisions = reversed(skip(self.reflog.walk(), 1))
        for rev in revisions:
            content = self.reflog.read(rev)
            info = json.loads(content)
            # Create and populate segment
            sgm = Segment(self.schema)
            for column, dig in info['columns'].items():
                prefix, suffix = dig[:2], dig[2:]
                sgm[column] = self.sgm_grp[prefix][suffix]


        # TODO concat!
        return sgm

    def write(self, sgm, idx_start=None, idx_end=None):
        info = {
            'idx_start': idx_start or sgm.idx_start,
            'idx_end': idx_end or sgm.idx_end,
            'columns': {},
        }
        for name, dig in sgm.hexdigests():
            prefix, suffix = dig[:2], dig[2:]
            sgm.copy(name, self.sgm_grp.require_group(prefix), suffix)
            info['columns'][name] = dig
        content = json.dumps(info)
        self.reflog.commit(content.encode())

    def squash(self, from_revision=None, to_revision=None):
        '''
        Collapse all revision between the two
        '''

        # TODO
