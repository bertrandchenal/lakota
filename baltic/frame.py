from bisect import bisect_left, bisect_right
from itertools import chain

import numexpr
from numpy import array_equal, asarray, concatenate

from .utils import hashed_path, hexdigest


class Frame:
    """
    DataFrame-like object
    """

    def __init__(self, schema, columns=None, pod=None):
        self.schema = schema
        self.columns = columns or {}
        self.pod = pod

    @classmethod
    def from_df(cls, schema, df):
        frm = cls(schema)
        frm.write(df)
        return frm

    @classmethod
    def from_pod(cls, schema, pod, digests, length):
        cols = {}
        for coldef, dig in zip(schema.columns.values(), digests):
            cols[coldef.name] = Column(
                coldef, Segment(digest=dig, pod=pod, length=length)
            )
        return Frame(schema, columns=cols, pod=pod)

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
        # [TODO] record start and stop when frame is created with
        # from_pod, and use it to bypass the test hereunder if start
        # and end are not interesting with frame.start and frame.stop

        if end is None:
            end = start
            closed = "both"
        else:
            closed = closed or "left"
        idx_start = self.index(*start, right=closed == "right")
        idx_end = self.index(*end, right=closed in ("both", "right"))
        return self.slice(slice(idx_start, idx_end))

    def slice(self, slc):
        """
        Slice between both position start and end
        """
        if slc.start == 0 and slc.stop >= len(self):
            return self
        new_frame = {}
        for name in self.schema.columns:
            new_frame[name] = self.columns[name].slice(slc)
        return Frame(self.schema, new_frame)

    @classmethod
    def concat(cls, schema, *frames):
        new_frame = {}
        for name in schema.columns:
            cols = [s.columns[name] for s in frames]
            new_col = Column.concat(cols)
            new_frame[name] = new_col
        return Frame(schema, new_frame)

    def __eq__(self, other):
        return all(array_equal(self[c], other[c]) for c in self.schema.columns)

    def __len__(self):
        if not self.columns:
            return 0
        name = next(iter(self.schema.columns))
        return len(self.columns[name])

    def keys(self):
        return self.schema.columns

    def write(self, df, reverse_idx=False):
        for name in self.schema.columns:
            arr = df[name]
            if hasattr(arr, "values"):
                arr = arr.values
            self[name] = arr

    def __setitem__(self, name, arr):
        # Make sure we have a numpy array
        arr = asarray(arr, dtype=self.schema[name].dt)
        sgm = Segment(arr=arr)
        self.columns[name] = Column(self.schema.columns[name], sgm)

    def __getitem__(self, name):
        return self.columns[name].read()

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
        # [TODO] use schema.serialize
        dt = self.schema[column].dt
        if dt in ("int", "int64"):
            # json does not like int64
            return int(value)
        return value

    def save(self, pod):
        all_dig = []
        for name, dig in self.hexdigests():
            all_dig.append(dig)
            arr = self[name]
            data = self.schema[name].encode(arr)
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
    def __init__(self, coldef, *segments):
        self.coldef = coldef
        self.segments = segments

    def slice(self, slc):
        segments = []
        for sgm in self.segments:
            segments.append(sgm.slice(slc))
            slc = slice(slc.start - len(sgm), slc.stop - len(sgm))

        return Column(self.coldef, *segments)

    @classmethod
    def concat(cls, columns):
        coldef = columns[0].coldef
        assert all(c.coldef == coldef for c in columns[1:])
        segments = list(chain.from_iterable(c.segments for c in columns))
        new_col = Column(coldef, *segments)
        return new_col

    def read(self):
        if len(self.segments) == 1:
            return self.segments[0].read(self.coldef)
        arrays = []
        for sgm in self.segments:
            arr = sgm.read(self.coldef)
            arrays.append(arr)
        return concatenate(arrays)

    def __len__(self):
        return sum(len(s) for s in self.segments)


class Segment:
    def __init__(self, arr=None, digest=None, pod=None, length=None):
        assert arr is not None or digest is not None
        self.arr = arr
        self.digest = digest
        self.pod = pod
        self.length = length if length is not None else len(arr)
        self.slc = None

    def read(self, coldef):
        if self.arr is None:
            folder, filename = hashed_path(self.digest)
            data = self.pod.cd(folder).read(filename)
            self.arr = coldef.decode(data)
        if self.slc is None:
            return self.arr
        return self.arr[self.slc]

    def slice(self, slc):
        sgm = Segment(self.arr, self.digest, self.pod, self.length)
        sgm.slc = slc
        return sgm

    def __len__(self):
        if self.slc is not None:
            return self.slc.stop - self.slc.start
        return self.length
