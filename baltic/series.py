from pathlib import Path
import json
import time

import zarr

from .reflog import RefLog
from .segment import Segment


def intersect(info, start, end):
    ok_start = not end or info['start'] <= end
    ok_end = not start or info['end'] >= start
    if not (ok_start and ok_end):
        return None
    # return reduced range
    return (max(info['start'], start), min(info['end'], end))


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

    def read(self, start=[], end=[]):
        '''
        Read all matching segment and combine them
        '''

        # Collect all rev info
        revisions = self.reflog.walk()
        series_info = []
        for rev in revisions:
            content = self.reflog.read(rev)
            info = json.loads(content)
            if intersect(info, start, end):
                series_info.append(info)
        # Order revision backward
        series_info = list(reversed(series_info))
        # Recursive discovery of matching segments
        segments = self._read(series_info, start, end)

        if not segments:
            return Segment(self.schema)
        return Segment.concat(self.schema, *segments)

    def _read(self, series_info, start, end):
        segments = []
        for pos, info in enumerate(series_info):
            match = intersect(info, start, end)
            if not match:
                continue

            # instanciate segment
            sgm = Segment.from_zarr(self.schema, self.sgm_grp,
                                    info['columns'], match)
            segments.append(sgm)

            mstart, mend = match
            # recurse left
            if mstart > start:
                left_sgm = self._read(series_info[pos+1:], start, mstart)
                segments = left_sgm + segments

            # recurse right
            if mend < end:
                right_sgm = self._read(series_info[pos+1:], mend, end)
                segments = segments + right_sgm

            break
        return segments

    def write(self, sgm, start=None, end=None):
        col_digests = sgm.save(self.sgm_grp)
        idx_start = start or sgm.start()
        idx_end = end or sgm.end()

        info = {
            'start': idx_start,
            'end': idx_end,
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
