from bisect import bisect_left, bisect_right
from collections import defaultdict
from functools import lru_cache

import numexpr
from numpy import array_equal, asarray, bincount, concatenate, lexsort, ndarray, unique

from .sexpr import AST
from .utils import Pool, hashed_path

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
            columns = schema.cast(columns)
        self.columns = columns

    @classmethod
    def from_segments(cls, schema, segments, limit=None, offset=None, select=None):
        if not segments:
            return Frame(schema)
        select = select or schema.columns

        with Pool() as pool:
            for name in schema.columns:
                if name not in select:
                    continue
                pool.submit(
                    Frame.read_segments, segments, name, limit=limit, offset=offset
                )

        res = dict(pool.results)
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
        # See also https://github.com/mapbox/snuggs
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
        for name in self.columns:
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

    def reduce(self, *col_list, **col_dict):
        """
        Return a new frame containing the choosen columns. A column can be
        one of the existing column or an s-expression that we be automatically
        evaluated.
        """

        # Merge all args in one dict
        columns = dict(zip(col_list, col_list))
        columns.update(col_dict)

        # Detect aggregations
        all_ast = {}
        for alias, expr in columns.items():
            if expr.startswith("("):
                all_ast[alias] = AST.parse(expr)
        agg_ast = {}
        other_ast = {}
        for alias, ast in all_ast.items():
            if ast.is_aggregate():
                agg_ast[alias] = ast
            else:
                other_ast[alias] = ast

        # TODO shortcut computated ig agg_ast is empty

        non_agg = {}
        for alias, expr in columns.items():
            if alias in agg_ast:
                continue
            ast = other_ast.get(alias)
            if ast:
                arr = ast.eval(env=self)
            else:
                arr = self.columns[expr]
            non_agg[alias] = arr

        # Compute binning
        bin_arrays = list(non_agg.values())
        keys, bins = unique(bin_arrays, return_inverse=True)

        res = {}
        for alias, arr in zip(non_agg, keys):
            res[alias] = keys

        # Compute aggregates
        env = dict(self)
        env.update({"_keys": keys, "_bins": bins})
        for alias, expr in agg_ast.items():
            arr = expr.eval(env)
            res[alias] = arr
        return Frame(self.schema, res)

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

    def drop(self, *columns):
        keep_columns = (c for c in self.columns if c not in columns)
        return self.select(*keep_columns)

    def __iter__(self):
        return iter(self.columns)

    def select(self, keep):
        cols = {k: v for k, v in self.columns.items() if k in keep}
        return Frame(self.schema, cols)

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
        self._read = lru_cache(len(schema.columns))(self._read)

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

    def _read(self, name):
        folder, filename = hashed_path(self.digest[name])
        data = self.pod.cd(folder).read(filename)
        return data

    def read(self, name, start=None, stop=None):
        data = self._read(name)
        arr = self.schema[name].decode(data)
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
