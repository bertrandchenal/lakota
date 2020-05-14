from bisect import bisect_left

from numpy import dtype, unique, array_equal
import zarr


class Segment:
    '''
    In-memory storage for one or more dataframe
    '''

    def __init__(self, schema, zarr_group=None):
        if zarr_group is None:
            # Create in-memory group
            zarr_group = zarr.group()
        elif not isinstance(zarr_group, zarr.Group):
            zarr_group = zarr.group(zarr_group)
        self.root = zarr_group
        self.schema = schema

    @classmethod
    def from_df(cls, schema, df):
        sgm = cls(schema)
        sgm.write(df)
        return sgm

    @classmethod
    def from_zarr(cls, schema, group, digests):
        sgm = cls(schema)
        for name, dig in zip(schema.columns, digests):
            prefix, suffix = dig[:2], dig[2:]
            zarr.copy(group[prefix][suffix], sgm.root, name, shallow=True)
        return sgm

    def __setitem__(self, name, arr):
        categ_like = (dtype('O'), dtype('U')) # should be more extensive
        if self.schema.dtype(name) in categ_like:
            categ = zarr.Categorize(unique(arr), dtype=object)
            self.root.array(name, arr, dtype=object, object_codec=categ)
        else:
            self.root.array(name, arr)

    def __eq__(self, other):
        return all(
            array_equal(self[c][:], other[c][:])
            for c in self.schema.columns)

    def write(self, df, reverse_idx=False):
        # TODO check no column is missing (at least in the index)
        for name in self.schema.columns:
            arr = df[name]
            if hasattr(arr, 'values'):
                arr = arr.values
            self[name] = arr

    def read(self, *names):
        cols = {}
        for name in names:
            arr = self[name]
            cols[name] = arr
        return cols

    def keys(self):
        return self.root.keys()

    def __getitem__(self, name):
        return self.root[name]

    def hexdigests(self):
        for name in self.schema.columns:
            yield name, self.root[name].hexdigest()

    def size(self):
        return sum(self.root[n].size for n in self.schema.columns)

    def start(self):
        '''
        Return a json serializable (monotonic) representation of the
        first value in the (sorted) index.
        '''
        return [self.serialize(n, self.root[n][0]) for n in self.schema.idx]

    def end(self):
        '''
        Return a json serializable (monotonic) representation of the
        latest value in the (sorted) index.
        '''
        return [self.serialize(n, self.root[n][-1]) for n in self.schema.idx]

    def serialize(self, column, value):
        dtype = self.schema.dtype(column)
        if dtype in ('int', 'int64'):
            # json does not like int64
            return int(value)
        return value

    def save(self, group):
        all_dig = []
        for name, dig in self.hexdigests():
            all_dig.append(dig)
            prefix, suffix = dig[:2], dig[2:]
            zarr.copy(self.root[name], group.require_group(prefix), suffix)
        return all_dig

    def index(self, name, value):
        return bisect_left(self.root[name], value)
