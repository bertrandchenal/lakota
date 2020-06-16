from urllib.parse import urlsplit, urlunparse

import zarr

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

    def __init__(self, path=None, cache=False):
        uri = urlsplit(path)
        if not uri.scheme:
            grp = zarr.group(path)
        elif uri.scheme == 's3':
            store = self.s3_store(uri, cache)
            grp = zarr.group(store=store)
        self.grp = grp
        self.schema_series = Series(self.schema,
                                    self.grp.require_group('registry'))
        self.series_root = self.grp.require_group('series')

    @classmethod
    def s3_store(cls, uri, cache):
        import s3fs
        # replace scheme with http
        parts = list(uri)
        # Change uri
        parts[0] = 'http'
        # change netloc
        hostname = uri.hostname or 'localhost'
        port = uri.port or 80
        parts[1] = f'{hostname}:{port}'
        # Make sure other parts are empty
        parts[2:] = [''] * 4
        endpoint_url = urlunparse(parts)

        # Connect to server
        s3 = s3fs.S3FileSystem(
            key=uri.username,
            secret=uri.password,
            client_kwargs={
                'endpoint_url': endpoint_url,
        })
        store = s3fs.S3Map(root=uri.path.strip('/'), s3=s3, check=False)

        if not cache:
            return store
        # Enable in-memory cache limited to 128MB
        return zarr.LRUStoreCache(store, max_size=128*1024*1014)

    def clear(self):
        for key in self.grp:
            del self.grp[key]

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
        series_group = self.series_root.require_group(prefix)
        series_group = series_group.require_group(suffix)
        series = Series(schema, series_group)
        return series

