from pathlib import Path
import json
import time

import zarr

from .reflog import RefLog
from .segment import Segment

# TODO: have a repo to store all the schema and one repo per
# timeseries

# OR: mix everything in one repo, and rely on 'fmt' in the the info file

# each repo can also be the two first letters of the digest of the
# label (but that would be taken cared outside of this class)


class Series:
    '''
    Combine a zarr group and a reflog to provide a versioned and
    concurrent management of timeseries.
    '''

    def __init__(self, schema, store=None):
        self.schema = schema
        if store:
            path = Path(store.root)
            reflog_store = zarr.DirectoryStore(path / 'refs')
            sgm_store = zarr.DirectoryStore(path / 'segments')
        else:
            reflog_store = zarr.MemoryStore()
            sgm_store = zarr.MemoryStore()

        self.reflog = RefLog(reflog_store)
        self.sgm_grp = zarr.group(store=sgm_store)
        self.schema = schema

    def read(self, start=None, stop=None):
        '''
        Read underlying array between start and stop
        '''
        # Follow revisions backwards
        revisions = self.reflog.walk()
        sgm = None
        for rev in revisions:
            content = self.reflog.read(rev)
            info = json.loads(content)
            # TODO Skip revision if no intersection with start/stop
            # (or masked by higher revision)

            # Create and populate segment
            sgm = Segment.from_zarr(self.schema, self.sgm_grp, info['columns'])

            # TODO stack relevant segments
            # sgm = Segment(self.schema)
            # for column, dig in info['columns'].items():
            #     prefix, suffix = dig[:2], dig[2:]
            #     sgm[column] = self.sgm_grp[prefix][suffix]

        if sgm is None:
            return Segment(self.schema)
        return sgm

    def write(self, sgm, start=None, end=None):
        col_digests = sgm.save(self.sgm_grp)
        idx_start = start or sgm.start()
        idx_end = end or sgm.end()

        info = {
            'idx_start': idx_start,
            'idx_end': idx_end,
            'fmt': 'segment.zarr', # not needed here should be saved in schema (or somewhere in reg)
            'size': sgm.size(), # needed to implement squashing strategies
            'timestamp': time.time(),
            'columns': col_digests,
        }
        content = json.dumps(info)
        self.reflog.commit(content.encode())

    def squash(self, from_revision=None, to_revision=None):
        '''
        Collapse all revision between the two
        '''

        # TODO
