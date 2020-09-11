from bisect import bisect_left, bisect_right
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import numexpr
from numpy import array_equal, asarray, bincount, concatenate, lexsort, ndarray, unique

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
        select = select or schema.columns

        futures = []
        with ThreadPoolExecutor(4) as pool:
            for name in schema.columns:
                if name not in select:
                    continue
                f = pool.submit(
                    Frame.read_segments, segments, name, limit=limit, offset=offset
                )
                futures.append(f)

        res = dict(f.result() for f in futures)
        return Frame(schema, res)

    @classmethod
    def read_segments(cls, segments, name, limit=None, offset=None):
        total_len = sum(len(s) for s in segments)
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
        return name, concatenate(arrays) if arrays else []

    def df(self, *columns):
        if DataFrame is None:
            raise ModuleNotFoundError("No module named 'pandas'")
        return DataFrame({c: self[c] for c in self.schema.columns})

    def lexsort(self):
        idx_cols = reversed(list(self.schema.idx))
        return lexsort([self[n] for n in idx_cols])

    @classmethod
    def concat(cls, *frames):
        # Corner cases
        if len(frames) == 0:
            return None
        if len(frames) == 1:
            return frames[0]

        # General cases
        schema = frames[0].schema
        cols = defaultdict(list)
        # Build dict of list
        for frm in frames:
            if not frm.schema == schema:
                raise ValueError("Unable to concat frames with different schema")
            for name in schema:
                arr = frm[name]
                if len(arr) == 0:
                    continue
                cols[name].append(arr)
        # Concatenate all lists
        for name in schema:
            cols[name] = concatenate(cols[name])
        # Create frame and sort it
        return Frame(schema, cols).sorted()

    def sorted(self):
        return self.mask(self.lexsort())

    def mask(self, mask):
        # if mask is a string, eval it first
        if isinstance(mask, str):
            mask = self.eval(mask)
        cols = {}
        # Apply mask to each column
        for name in self.columns:
            arr = self.columns[name]
            if len(arr) == 0:
                continue
            cols[name] = arr[mask]
        # Return new frame
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
            right = closed in ("right", None)
            idx_start = self.index(start, right=right)
        if stop:
            right = closed in ("both", "right")
            idx_stop = self.index(stop, right=right)
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
        values, *_ = self.columns.values()
        return len(values)

    def keys(self):
        return iter(self.columns)

    def start(self):
        return self.schema.row(self, pos=0, full=False)

    def stop(self):
        return self.schema.row(self, pos=-1, full=False)

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
        if by in self.columns:
            return self.columns[by]
        return self.schema[by].cast([])

    def drop(self, *column_names):
        names = (c for c in self.columns if c not in column_names)
        frm = Frame(self.schema, {c: self.columns[c] for c in names})
        return frm

    def __iter__(self):
        return iter(self.columns)

    def reduce(self):
        # Index columns that are present in the frame
        idx_cols = [c for c in self.schema.idx if c in self.columns]

        # Handle corner cases
        if len(idx_cols) == 0:
            return Frame(self.schema, {c: [self[c].sum()] for c in self.columns})
        elif len(idx_cols) == len(self.schema.idx):
            # Full index, unicity is guaranteed
            return self

        # Handle general case
        if len(idx_cols) == 1:
            partial_index = self[idx_cols[0]]
            axis = 0
        else:
            partial_index = asarray([self[c] for c in idx_cols])
            axis = 1
        keys, bins = unique(partial_index, axis=axis, return_inverse=True)

        res = {}
        for other_col in self.columns:
            if other_col in idx_cols:
                continue
            res[other_col] = bincount(bins, weights=self[other_col])

        if len(idx_cols) == 1:
            res[idx_cols[0]] = keys
        else:
            for pos, col in enumerate(idx_cols):
                res[col] = keys[pos]

        # TODO handle other aggregate (beside a simple sum)
        # TODO adapt scheme (avg gives floats count gives integers)

        return Frame(self.schema, res)

    def __repr__(self):
        res = []
        for name in self:
            res.append(name + "-> " + str(self[name]))
        return "\n".join(res)


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
            frm = Frame(
                self.schema,
                {name: self.read(name) for name in self.schema},
            )
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
