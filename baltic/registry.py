from pathlib import PurePosixPath

import fsspec

from .segment import Segment
from .schema import Schema
from .series import Series
from .utils import hexdigest


# Idea: "package" a bunch of writes in a ZipStore and send the
# zipstore on s3

class Registry:
    '''
    Use a Series object to store all the series labels
    '''

    schema = Schema(['label:str', 'schema:str'])

    def __init__(self, uri=None, **fs_kwargs):
        if not uri:
            protocol = 'memory'
            path = '.'
        else:
            protocol, path = uri.split('://', 1)
        self.path = PurePosixPath(path)
        self.fs = fsspec.filesystem(protocol, **fs_kwargs)

        self.schema_series = Series(self.schema, self.fs, self.path / 'registry' )
        self.series_root = self.path / 'series'

    def clear(self):
        for key in self.fs.ls(self.path):
            self.fs.rm(self.path / key)

    def create(self, schema, *labels):
        # FIXME prevent double create (here or in the segment)
        sgm = Segment.from_df(
            self.schema,
            {
                'label': labels,
                'schema': [schema.dumps()] * len(labels)
            })
        self.schema_series.write(sgm) # SQUASH ?

    def get(self, label):
        sgm = self.schema_series.read()
        idx = sgm.index(label)
        assert sgm['label'][idx] == label
        schema = Schema.loads(sgm['schema'][idx])
        digest = hexdigest(label.encode())
        prefix, suffix = digest[:2], digest[2:]
        series = Series(schema, self.fs, self.series_root / prefix / suffix)
        return series
