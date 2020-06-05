from bisect import bisect_left, bisect_right

from numpy import unique, array_equal
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
            zarr.copy(group[prefix][suffix], sgm.root, name)
        return sgm

    def df(self):
        from pandas import DataFrame
        return DataFrame(dict(self))

    def slice(self, start, end):
        new_group = zarr.group()
        idx_start = self.index(*start)
        idx_end = self.index(*end)
        for name in self.schema.columns:
            sl = self.root[name][idx_start:idx_end]
            new_group.array(name, sl, dtype=self.schema.dtype(name))

        return Segment(self.schema, new_group)

    @classmethod
    def concat(cls, schema, *segments):
        new_len = sum(len(s) for s in segments)
        new_group = zarr.group()
        for name in schema.columns:
            new_group.empty(name, shape=new_len, dtype=schema.dtype(name))
            idx_start = 0
            for s in segments:
                idx_end = idx_start + len(s)
                new_group[name][idx_start:idx_end] = s[name]
                idx_start = idx_end

        return Segment(schema, new_group)

    def __setitem__(self, name, arr):
        categ_like = ('O', 'U') # should be more extensive
        dt = self.schema.dtype(name)
        if dt in categ_like:
            categ = zarr.Categorize(unique(arr), dtype=object)
            self.root.array(name, arr, dtype=object, object_codec=categ)
        else:
            self.root.array(name, arr, dtype=dt) #XXX chunks=500_000

    def __eq__(self, other):
        return all(
            array_equal(self[c][:], other[c][:])
            for c in self.schema.columns)
    def __len__(self):
        name = self.schema.columns[0]
        return len(self.root[name])

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
        dt = self.schema.dtype(column)
        if dt in ('int', 'int64'):
            # json does not like int64
            return int(value)
        return value

    def save(self, group):
        all_dig = []
        for name, dig in self.hexdigests():
            all_dig.append(dig)
            prefix, suffix = dig[:2], dig[2:]
            try:
                sg = group.require_group(prefix)
                zarr.copy(self.root[name], sg, suffix, if_exists='skip')
            except ValueError:
                print(name)
                # Zarr does not raise anything more precice than a
                # ValueError when a group already exists
                pass
        return all_dig

    def index(self, *values):
        if not values:
            return None
        lo = 0
        hi = len(self)
        for name, val in zip(self.schema.idx, values):
            lo = bisect_left(self.root[name], val, lo=lo, hi=hi)
            hi = bisect_right(self.root[name], val, lo=lo, hi=hi)
        return lo
