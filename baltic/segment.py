from bisect import bisect_left, bisect_right
from hashlib import sha1

from numpy import array_equal, asarray, empty


class Segment:
    '''
    In-memory storage for one or more dataframe
    '''

    def __init__(self, schema, frame=None):
        self.schema = schema
        self.frame = frame or {}

    @classmethod
    def from_df(cls, schema, df):
        sgm = cls(schema)
        sgm.write(df)
        return sgm

    @classmethod
    def from_fs(cls, schema, fs, path, digests):
        sgm = cls(schema)
        for name, dig in zip(schema.columns, digests):
            prefix, suffix = dig[:2], dig[2:]
            data = fs.open(path / prefix / suffix).read()
            arr = schema.decode(name, data)
            sgm.frame[name] = arr
        return sgm

    def df(self):
        from pandas import DataFrame
        return DataFrame(dict(self))

    def slice(self, start, end):
        new_frame = {}
        idx_start = self.index(*start)
        idx_end = self.index(*end)
        for name in self.schema.columns:
            # FIXME DECODE
            sl = self.frame[name][idx_start:idx_end]
            new_frame[name] = sl

        return Segment(self.schema, new_frame)

    @classmethod
    def concat(cls, schema, *segments):
        new_len = sum(len(s) for s in segments)
        new_frame = {}
        for name in schema.columns:
            arr = empty(new_len, dtype=schema.dtype(name))
            idx_start = 0
            for s in segments:
                arr = s[name]
                idx_end = idx_start + len(arr)
                arr[idx_start:idx_end] = arr
                idx_start = idx_end
            new_frame[name] = arr
        return Segment(schema, new_frame)

    def __setitem__(self, name, arr):
        # dt = self.schema.dtype(name)
        # Make sure we have a numpy array
        arr = asarray(arr, dtype=self.schema.dtype(name))
        self.frame[name] = arr

    def __eq__(self, other):
        return all(
            array_equal(self[c][:], other[c][:])
            for c in self.schema.columns)

    def __len__(self):
        name = self.schema.columns[0]
        return len(self.frame[name])

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
        return self.frame.keys()

    def __getitem__(self, name):
        return self.frame[name]

    def hexdigests(self):
        for name in self.schema.columns:
            arr = self.frame[name]
            yield name, sha1(arr).hexdigest()

    def size(self):
        return sum(self.frame[n].size for n in self.schema.columns)

    def read_at(self, pos):
        '''
        Return a json serializable (monotonic) representation of the value
        at the given position in the (sorted) index.
        '''
        res = []
        for n in self.schema.idx:
            arr = self.frame[n]
            res.append(self.serialize(n, arr[pos]))
        return res

    def start(self):
        return self.read_at(0)

    def end(self):
        return self.read_at(-1)

    def serialize(self, column, value):
        dt = self.schema.dtype(column)
        if dt in ('int', 'int64'):
            # json does not like int64
            return int(value)
        return value

    def save(self, fs, path):
        all_dig = []
        for name, dig in self.hexdigests():
            all_dig.append(dig)
            prefix, suffix = dig[:2], dig[2:]
            arr = self.frame[name]
            data = self.schema.encode(name, arr)
            fs.open(path / prefix/ suffix, 'wb').write(data) #if_exists='skip')
        return all_dig

    def index(self, *values):
        if not values:
            return None
        lo = 0
        hi = len(self)
        for name, val in zip(self.schema.idx, values):
            arr = self.frame[name]
            lo = bisect_left(arr, val, lo=lo, hi=hi)
            hi = bisect_right(arr, val, lo=lo, hi=hi)
        return lo
