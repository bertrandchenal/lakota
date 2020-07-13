from bisect import bisect_left, bisect_right

import numexpr
from numpy import array_equal, asarray, concatenate

from .utils import hexdigest, hashed_path


class Segment:
    """
    In-memory storage for one or more dataframe
    """

    def __init__(self, schema, frame=None):
        self.schema = schema
        self.frame = frame or {c: [] for c in schema.columns}

    @classmethod
    def from_df(cls, schema, df):
        sgm = cls(schema)
        sgm.write(df)
        return sgm

    @classmethod
    def from_pod(cls, schema, pod, digests):
        sgm = cls(schema)
        for name, dig in zip(schema.columns, digests):
            # TODO abstract somewhere the prefix/suffix hashing
            folder, filename = hashed_path(dig)
            data = pod.cd(folder).read(filename)
            arr = schema.decode(name, data)
            sgm.frame[name] = arr
        return sgm

    def df(self):
        from pandas import DataFrame

        return DataFrame(dict(self))

    def mask(self, mask_arr):
        new_frame = {n: arr[mask_arr] for n, arr in self.frame.items()}
        return Segment(self.schema, new_frame)

    def eval(self, expr):
        res = numexpr.evaluate(expr, local_dict=self.frame)
        return res

    def empty(self):
        return len(self) == 0

    def slice(self, start, end=None, closed=None):
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
        new_frame = {}
        idx_start = self.index(*start, right=closed == "right")
        idx_end = self.index(*end, right=closed in ("both", "right"))
        for name in self.schema.columns:
            sl = self.frame[name][idx_start:idx_end]
            new_frame[name] = sl

        return Segment(self.schema, new_frame)

    @classmethod
    def concat(cls, schema, *segments):
        new_frame = {}
        for name in schema.columns:
            new_arr = concatenate([s[name] for s in segments])
            new_frame[name] = new_arr.astype(schema.dtype(name))
        return Segment(schema, new_frame)

    def __setitem__(self, name, arr):
        # dt = self.schema.dtype(name)
        # Make sure we have a numpy array
        arr = asarray(arr, dtype=self.schema.dtype(name))
        self.frame[name] = arr

    def __eq__(self, other):
        return all(array_equal(self[c], other[c]) for c in self.schema.columns)

    def __len__(self):
        name = self.schema.columns[0]
        return len(self.frame[name])

    def write(self, df, reverse_idx=False):
        for name in self.schema.columns:
            arr = df[name]
            if hasattr(arr, "values"):
                arr = arr.values
            self[name] = arr

    def keys(self):
        return self.frame.keys()

    def __getitem__(self, name):
        return self.frame[name]

    def hexdigests(self):
        for name in self.schema.columns:
            arr = self.frame[name]
            res = name, hexdigest(arr.tostring())
            yield res

    def size(self):
        return sum(self.frame[n].size for n in self.schema.columns)

    def read_at(self, pos):
        """
        Return a json serializable (monotonic) representation of the value
        at the given position in the (sorted) index.
        """
        return tuple(self.serialize(n, self.frame[n][pos]) for n in self.schema.idx)

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
            arr = self.frame[name]
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
            arr = self.frame[name]
            lo = bisect_left(arr, val, lo=lo, hi=hi)
            hi = bisect_right(arr, val, lo=lo, hi=hi)
        if right:
            return hi
        return lo
