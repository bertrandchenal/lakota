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
    def from_segments(cls, schema, segments, limit=None, offset=None, select=None):
        if not segments:
            return Frame(schema)
        total_len = sum(len(s) for s in segments)
        select = select or schema.columns
        columns = {}
        for name in schema.columns:
            if name not in select:
                continue
            arrays = []
            start = offset or 0
            stop = total_len + 1 if limit is None else start + limit
            for sgm in segments:
                if stop == 0:
                    break
                if start >= len(sgm):
                    start = max(start - len(sgm), 0)
                    stop = max(stop - len(sgm), 0)
                    continue
                arr = sgm.read(name, start=start, stop=stop)
                start = max(start - len(sgm), 0)
                stop = max(stop - len(sgm), 0)
                arrays.append(arr)

            columns[name] = concatenate(arrays) if arrays else []
        return Frame(schema, columns)

    def df(self, *columns):
        if DataFrame is None:
            raise ModuleNotFoundError("No module named 'pandas'")
        return DataFrame({c: self[c] for c in self.schema.columns})

    def mask(self, mask):
        if isinstance(mask, str):
            mask = self.eval(mask)
        cols = {}
        for name in self.columns:
            arr = self.columns[name]
            if len(arr) == 0:
                continue
            cols[name] = arr[mask]
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
    def __init__(self, schema, pod, digests, start, stop, length):
        self.schema = schema
        self.pod = pod
        self.start = start
        self.stop = stop
        self.length = length
        self.digest = dict(zip(schema, digests))
        self._array_cache = {}

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
            stop >= self.stop if closed in ("both", "right") else stop > self.stop
        )
        if all(skip_tests):
            return self
        else:
            # Materialize arrays
            frm = Frame(self.schema, {name: self.read(name) for name in self.schema},)
            # Compute slice and apply it
            frm = frm.index_slice(start, stop, closed=closed)
            return Segment(start, stop, frm)

    def __len__(self):
        return self.length

    def read(self, name, start=None, stop=None):
        arr = self._array_cache.get(name)
        if arr is None:
            folder, filename = hashed_path(self.digest[name])
            data = self.pod.cd(folder).read(filename)
            arr = self.schema[name].decode(data)
            self._array_cache[name] = arr
        if start is stop is None:
            return arr
        return arr[start:stop]

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

    def read(self, name, start, stop):
        return self.frm[name][start:stop]


class EmptySegment:
    def __init__(self, start, stop, schema):
        self.start = start
        self.stop = stop
        self.schema = schema

    def __len__(self):
        return 0

    def read(self, name, start, stop):
        return self.schema[name].cast([])

    @property
    def empty(self):
        return True
