from bisect import bisect_left, bisect_right

import numexpr
from numpy import array_equal, concatenate, ndarray

from .utils import hashed_path

try:
    from pandas import DataFrame
except ImportError:
    DataFrame = None


class Frame:
    """
    DataFrame-like object
    """

    def __init__(self, schema, columns=None):
        self.schema = schema
        if DataFrame is not None and isinstance(columns, DataFrame):
            columns = {c: columns[c].values for c in columns}
        else:
            columns = schema.cast(columns or {})
        self.columns = columns

    @classmethod
    def from_segments(cls, schema, *segments, head=None, tail=None):
        if not segments:
            return Frame(schema)
        total_len = sum(len(s) for s in segments)
        columns = {}
        for name in schema.columns:
            arrays = []
            h = head
            t = total_len - tail if tail is not None else None
            for sgm in segments:
                arrs = sgm.read(name, head=h, tail=t)
                if h is not None:
                    h = max(h - len(sgm), 0)
                if t is not None:
                    t = max(t - len(sgm), 0)
                arrays.extend(arrs)
            columns[name] = concatenate(arrays)
        return Frame(schema, columns)

    def df(self, *columns):
        if DataFrame is None:
            raise ModuleNotFoundError("No module named 'pandas'")
        return DataFrame({c: self[c] for c in self.schema.columns})

    def mask(self, mask):
        if isinstance(mask, str):
            mask = self.eval(mask)
        cols = {name: self.columns[name][mask] for name in self.columns}
        return Frame(self.schema, cols)

    def eval(self, expr):
        res = numexpr.evaluate(expr, local_dict=self)
        return res

    @property
    def empty(self):
        return len(self) == 0

    def rowdict(self, *idx):
        pos = self.index(*self.schema.deserialize(idx))
        values = self.schema.row(pos, full=True)
        return dict(zip(self.schema.columns, values))

    def index_slice(self, start=None, stop=None, closed="left"):
        """
        Slice between two index value. `closed` can be "left" (default),
        "right" or "both".
        """
        idx_start = idx_stop = None
        if start:
            idx_start = self.index(start, right=closed == "right")
        if stop:
            idx_stop = self.index(stop, right=closed in ("both", "right"))
        return self.slice(idx_start, idx_stop)

    def index(self, values, right=False):
        if not values:
            return None
        lo = 0
        hi = len(self)
        for name, val in zip(self.schema.idx, values):
            arr = self.columns[name]
            lo = bisect_left(arr, val, lo=lo, hi=hi)
            hi = bisect_right(arr, val, lo=lo, hi=hi)

        if right:
            return hi
        return lo

    def slice(self, start=None, stop=None):
        """
        Slice between both position start and stop
        """
        # Replace None by actual values
        slc = slice(*(slice(start, stop).indices(len(self))))
        # Build new frame
        cols = {}
        for name in self.schema.columns:
            cols[name] = self.columns[name][slc]
        return Frame(self.schema, cols)

    def __eq__(self, other):
        return all(array_equal(self[c], other[c]) for c in self.schema.columns)

    def __len__(self):
        if not self.columns:
            return 0
        name = next(iter(self.schema.columns))
        return len(self.columns[name])

    def keys(self):
        return self.schema.columns

    def __setitem__(self, name, arr):
        # Make sure we have a numpy array
        arr = self.schema[name].cast(arr)
        if len(arr) != len(self):
            raise ValueError("Lenght mismatch")
        self.columns[name] = arr

    def __getitem__(self, by):
        # By slice -> return a frame
        if isinstance(by, slice):
            start = by.start and self.schema.deserialize(by.start)
            stop = by.stop and self.schema.deserialize(by.stop)
            return self.index_slice(start, stop)
        # By mask -> return a frame
        if isinstance(by, ndarray):
            return self.mask(by)
        # By column name -> return an array
        return self.columns[by]


class ShallowSegment:
    def __init__(self, schema, pod, digests, start, stop, length, closed="left"):
        self.schema = schema
        self.pod = pod
        self.start = start
        self.stop = stop
        self.length = length
        self.digest = dict(zip(schema, digests))

    def slice(self, start, stop, closed="left"):
        assert stop >= start
        # empty_test contains any condition that would result in an empty segment
        empty_test = [
            start > self.stop,
            stop < self.start,
            start == self.stop and closed not in ("both", "left"),
            stop == self.start and closed not in ("both", "right"),
        ]
        if any(empty_test):
            return EmptySegment(start, stop, self.schema)

        # skip_tests list contains all the tests that have to be true to
        # _not_ do the slice and return self
        skip_tests = (
            [start <= self.start]
            if closed in ("both", "left")
            else [start < self.start]
        )
        skip_tests.append(
            stop >= self.stop if closed in ("both", "righ") else stop > self.stop
        )
        if all(skip_tests):
            return self
        else:
            # Materialize arrays
            frm = Frame(
                self.schema,
                {name: concatenate(self.read(name)) for name in self.schema},
            )
            # Compute slice and apply it
            frm = frm.index_slice(start, stop, closed=closed)
            return Segment(start, stop, frm)

    def __len__(self):
        return self.length

    def read(self, name, head=None, tail=None):
        folder, filename = hashed_path(self.digest[name])
        data = self.pod.cd(folder).read(filename)
        arr = self.schema[name].decode(data)

        res = []
        if head:
            res.append(arr[:head])
        if tail:
            res.append(arr[tail:])
        if not res:
            res = [arr]
        return res

    @property
    def empty(self):
        return self.length == 0


class Segment:
    def __init__(self, start, stop, frm):
        self.start = start
        self.stop = stop
        self.frm = frm

    def __len__(self):
        return len(self.frm)

    def read(self, name, head=None, tail=None):
        res = []
        if head:
            res.append(self.frm[name][:head])
        if tail:
            res.append(self.frm[name][tail:])
        if not res:
            res = [self.frm[name]]
        return res


class EmptySegment:
    def __init__(self, start, stop, schema):
        self.start = start
        self.stop = stop
        self.schema = schema

    def __len__(self):
        return 0

    def read(self, name, head=None, tail=None):
        return [self.schema[name].cast([])]

    @property
    def empty(self):
        return True
