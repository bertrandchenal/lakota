import zarr

from .segment import Segment
from .schema import Schema
from .series import Series



class Registry:
    '''
    Use a Series object to store all the series labels
    '''

    schema = Schema(['label:str', 'schema:str'])

    def __init__(self, path=None):
        self.grp = zarr.group(path)
        if path:
            self.schema_series = Series(self.schema, path / 'registry')
            self.series_store = zarr.DirectoryStore(path / 'series')
        else:
            self.schema_series = Series(self.schema)
            self.series_store = zarr.MemoryStore()

    def create(self, schema, *labels):
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
        series = Series(schema, self.series_store)
        return series

    def squash(self, from_revision=None, to_revision=None):
        '''
        Collapse all revision between the two
        '''

        # TODO
