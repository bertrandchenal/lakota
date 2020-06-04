import zarr

from .segment import Segment
from .schema import Schema
from .series import Series
from .utils import hexdigest


class Registry:
    '''
    Use a Series object to store all the series labels
    '''

    schema = Schema(['label:str', 'schema:str'])

    def __init__(self, path=None):
        self.grp = zarr.group(path)
        self.schema_series = Series(self.schema,
                                    self.grp.require_group('registry'))
        self.series_root = self.grp.require_group('series')

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
        digest = hexdigest(label.encode())
        prefix, suffix = digest[:2], digest[2:]
        series_group = self.series_root.require_group(prefix)
        series_group = series_group.require_group(suffix)
        series = Series(schema, series_group)
        return series

