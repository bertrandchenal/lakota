from bisect import bisect_left, bisect_right
from itertools import chain

import numexpr
from numpy import array_equal, asarray, concatenate

from .utils import hashed_path, hexdigest


class Frame:
    """
    In-memory storage for one or more dataframe
    """

    def __init__(self, schema, frame=None):
        self.schema = schema
        self.frame = frame or {}

    @classmethod
    def from_df(cls, schema, df):
        sgm = cls(schema)
        sgm.write(df)
        return sgm

    @classmethod
    def from_pod(cls, schema, pod, digests):
        # Create a shallow frame that will read content from pod only when a column is accessed
        digests = dict(zip(schema.columns, digests))
        frame = {c: Column(c, schema, digests=digests[c], pod=pod) for c in schema.columns}
        sgm = cls(schema, frame)
        return sgm

    def df(self):
        from pandas import DataFrame
        return DataFrame(dict(self))

    def mask(self, mask_arr):
        new_frame = {n: self[n][mask_arr] for n in self.schema.columns}
        return Frame(self.schema, new_frame)

    def eval(self, expr):
        res = numexpr.evaluate(expr, local_dict=self)
        return res

    def empty(self):
        return len(self) == 0

    def index_slice(self, start, end=None, closed=None):
        """
        Slice between two index value. `closed` can be "left" (default),
        "right" or "both". If end is None, the code will use `start`
        as value and enforce "both" as value for `closed`
        """
        if end is None:
            end = start
            closed = "both"
        else:
            closed = closed or "left"
        idx_start = self.index(*start, right=closed == "right")
        idx_end = self.index(*end, right=closed in ("both", "right"))
        return self.slice(idx_start, idx_end)

    def slice(self, start, end):
        '''
        Slice between both position start and end
        '''
        if start == 0 and end >= len(self):
            return self
        new_frame = {}
        for name in self.schema.columns:
            col = self.frame[name].slice(start, end)
            new_frame[name] = col
        return Frame(self.schema, new_frame)

    @classmethod
    def concat(cls, schema, *frames):
        new_frame = {}
        for name in schema.columns:
            cols = [s.frame[name] for s in frames]
            new_col = Column.concat(cols)
            new_frame[name] = new_col
        return Frame(schema, new_frame)

    def __setitem__(self, name, arr):
        # Make sure we have a numpy array
        arr = asarray(arr, dtype=self.schema.dtype(name))
        self.frame[name] = Column(name, self.schema, arr=arr)

    def __eq__(self, other):
        return all(array_equal(self[c], other[c]) for c in self.schema.columns)

    def __len__(self):
        if not self.frame:
            return 0
        name = self.schema.columns[0]
        return len(self[name])

    def write(self, df, reverse_idx=False):
        for name in self.schema.columns:
            arr = df[name]
            if hasattr(arr, "values"):
                arr = arr.values
            self[name] = arr

    def keys(self):
        return self.schema.columns

    def __getitem__(self, name):
        return self.frame[name].read()

    def hexdigests(self):
        for name in self.schema.columns:
            arr = self[name]
            res = name, hexdigest(arr.tostring())
            yield res

    def read_at(self, pos):
        """
        Return a json serializable (monotonic) representation of the index
        at the given position in the (sorted) index.
        """
        return tuple(self.serialize(n, self[n][pos]) for n in self.schema.idx)

    def start(self):
        return self.read_at(0)

    def end(self):
        return self.read_at(-1)

    def serialize(self, column, value):
        dt = self.schema.dtype(column)
        if dt in ("int", "int64"):
            # json does not like int64
            return int(value)
        return value

    def save(self, pod):
        all_dig = []
        for name, dig in self.hexdigests():
            all_dig.append(dig)
            arr = self[name]
            data = self.schema.encode(name, arr)
            folder, filename = hashed_path(dig)
            pod.cd(folder).write(filename, data)  # if_exists='skip')
        return all_dig

    def index(self, *values, right=False):
        if not values:
            return None
        lo = 0
        hi = len(self)
        for name, val in zip(self.schema.idx, values):
            arr = self[name]
            lo = bisect_left(arr, val, lo=lo, hi=hi)
            hi = bisect_right(arr, val, lo=lo, hi=hi)
        if right:
            return hi
        return lo


class Column:
    def __init__(self, name, schema, arr=None, pod=None, digests=None, slice_start=None, slice_end=None):
        self.schema = schema
        self.name = name
        self.arr = arr
        self.digests = [digests] if isinstance(digests, str) else digests
        self.pod = pod
        self.slice_start = slice_start
        self.slice_end = slice_end

    def read(self):
        if self.arr is None:
            arrays = []
            # Construct array based on list of digests
            for dig in self.digests:
                folder, filename = hashed_path(dig)
                data = self.pod.cd(folder).read(filename)
                arrays.append(self.schema.decode(self.name, data))
            arr = concatenate(arrays)
            # Apply slice
            if self.slice_start or self.slice_end:
                arr = arr[self.slice_start:self.slice_end]
            self.arr = arr.astype(self.schema.dtype(self.name))
        return self.arr

    def slice(self, start, end):
        if self.arr is not None:
            return Column(self.name, self.schema, arr=self.arr[start:end])

        return Column(self.name, self.schema, pod=self.pod,
                      digests=self.digests, slice_start=start,
                      slice_end=end)

    @classmethod
    def concat(cls, columns):
        # TODO assert name and pod are shared
        name = columns[0].name
        pod = columns[0].pod
        schema = columns[0].schema
        # If at least on column is materialized, whe have to
        # materialize everyone
        if any(not c.digests for c in columns):
            arr = concatenate([c.read() for c in columns])
            new_col = Column(name, schema, arr=arr)
        else:
            digests = list(chain.from_iterable(c.digests for c in columns))
            new_col = Column(name, schema, pod=pod, digests=digests)
        return new_col
