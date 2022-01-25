from bisect import bisect_left, bisect_right
from collections import defaultdict

from numpy import (
    arange,
    argsort,
    array_equal,
    asarray,
    concatenate,
    ndarray,
    rec,
    unique,
)

from .schema import Schema
from .sexpr import AST, Alias
from .utils import Closed, Pool, as_tz, floor, pivot, pretty_nb

try:
    from pandas import DataFrame
except ImportError:
    DataFrame = None

__all__ = ["Frame"]


class Frame:
    """
    DataFrame-like object
    """

    _base_env = {
        "floor": floor,
        "pretty_nb": lambda xs: asarray(list(map(pretty_nb, xs))),
        "as-tz": as_tz,
    }

    def __init__(self, schema, columns=None):
        self.schema = schema
        if DataFrame is not None and isinstance(columns, DataFrame):
            columns = {c: columns[c].values for c in columns}
        self.columns = schema.cast(
            columns
        )  # XXX create empty list if one column is missing ?
        self.env = {}

    @classmethod
    def from_records(self, schema, records):
        return Frame(schema, pivot(records, list(schema)))

    @classmethod
    def from_segments(cls, schema, segments, limit=None, offset=None, select=None):
        if not select:
            select = schema.columns
        else:
            select = [select] if isinstance(select, str) else select

        start = offset or 0
        stop = None if limit is None else start + limit
        frames = []

        for sgm in segments:
            if stop == 0:
                break
            if start >= len(sgm):
                start = max(start - len(sgm), 0)
                if stop is not None:
                    stop = max(stop - len(sgm), 0)
                continue
            with Pool() as pool:
                # For each column we schedule a lambda that return a tuple
                # `(name, numpy_array)`
                read_col = lambda name: (
                    name,
                    sgm.read(name, start_pos=start, stop_pos=stop)
                )
                for name in select:
                    pool.submit(read_col, name)
            values = dict(pool.results)
            frames.append(Frame(schema, values))

            start = max(start - len(sgm), 0)
            if stop is not None:
                stop = max(stop - len(sgm), 0)

        # Return collected frames
        if frames:
            return Frame.concat(*frames)

        # Return empty frame
        frm = Frame(schema)
        if select:
            frm = frm.select(select)
        return frm


    def df(self, *columns):
        if DataFrame is None:
            raise ModuleNotFoundError("No module named 'pandas'")
        return DataFrame({c: self[c] for c in self.schema.columns if c in self.columns})

    def argsort(self, *sort_columns):
        sort_columns = sort_columns or list(self.schema.idx)
        arr = rec.fromarrays([self[n] for n in sort_columns], names=sort_columns)
        # Mergesort is faster on pre-sorted arrays
        return argsort(arr, kind="mergesort")

    def is_sorted(self):
        idx_cols = list(self.schema.idx)
        if len(idx_cols) == 1:
            arr = self[idx_cols[0]]
            return all(arr[1:] >= arr[:-1])

        # Multi-column index we fallback on argsort
        arr = rec.fromarrays([self[n] for n in idx_cols], names=idx_cols)
        sort_mask = self.argsort()
        a_range = arange(len(sort_mask))
        return all(sort_mask == a_range)

    @classmethod
    def concat(cls, *frames):
        # Corner cases
        if len(frames) == 0:
            return None
        if len(frames) == 1:
            return frames[0]

        # General cases
        schema = frames[0].schema
        names = list(frames[0])
        cols = defaultdict(list)
        # Build dict of list
        for frm in frames:
            if not frm.schema == schema:
                raise ValueError("Unable to concat frames with different schema")
            for name in names:
                arr = frm[name]
                if len(arr) == 0:
                    continue
                cols[name].append(arr)
        # Concatenate all lists
        for name in names:
            cols[name] = concatenate(cols[name])
        # Create frame and sort it
        return Frame(schema, cols).sorted()

    def sorted(self, *sort_columns):
        return self.mask(self.argsort(*sort_columns))

    def mask(self, mask, env=None):
        # if mask is a string, eval it first
        if isinstance(mask, str):
            mask = self.eval(mask, env=env)
        cols = {}
        # Apply mask to each column
        for name in self.columns:
            arr = self.columns[name]
            if len(arr) == 0:
                continue
            cols[name] = arr[mask]
        # Return new frame
        return Frame(self.schema, cols)

    def eval(self, expr, env=None):
        ast = AST.parse(expr)
        eval_env = self.eval_env()
        if env is not None:
            eval_env.update(env)
        res = ast.eval(eval_env)
        return res

    def eval_env(self):
        return {**self._base_env, **self.env, "self": self}

    @property
    def empty(self):
        return len(self) == 0

    def slice_index(self, start=None, stop=None, closed="l"):
        """
        Return slice postions between two index value. `closed` can be "l"
        (left, the default), "r" (right) "n" (none) or "b" (both).
        """
        closed = closed if isinstance(closed, Closed) else Closed[closed]
        idx_start = idx_stop = None
        if start:
            idx_start = self.index(start, right=not closed.left)
        if stop:
            idx_stop = self.index(stop, right=closed.right)
        return idx_start, idx_stop

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

    def islice(self, start=None, stop=None, closed="l"):
        """
        Return slice between two index values `start` and `stop`. It
        simply combines `self.slice` and `self.slice_index`
        """
        return self.slice(*self.slice_index(start, stop, closed))

    def __eq__(self, other):
        other = self.schema.cast(other)
        return all(array_equal(self[c], other[c]) for c in self.schema.columns)

    def get(self, name, default=None):
        return self.columns.get(name, default)

    def __contains__(self, column):
        return column in self.columns

    def __len__(self):
        if not self.columns:
            return 0
        values, *_ = self.columns.values()
        return len(values)

    def keys(self):
        return list(self.columns)

    def values(self, map_dtype=None):
        """
        Return iterator on frame columns. If given, `map_dtype` will also
        convert the type of returned arrays (see `map_dtype` method on
        `SchemaColumn`).
        """
        if not map_dtype:
            return list(self.columns.values())
        return [self.schema[k].map_dtype(self[k], style=map_dtype) for k in self.keys()]

    def records(self, map_dtype="default"):
        """
        Return a list of dict. If `map_dtype` is set, the values of the
        dicts will be typed based on selected style (see `map_dtype`
        method on `SchemaColumn`)
        """
        keys = self.keys()
        for vals in zip(*self.values(map_dtype=map_dtype)):
            yield dict(zip(keys, vals))

    def start(self):
        return self.schema.row(self, pos=0, full=False)

    def stop(self):
        return self.schema.row(self, pos=-1, full=False)

    def __setitem__(self, name, arr):
        # Make sure we have a numpy array
        arr = self.schema[name].cast(arr)
        if len(arr) != len(self):
            raise ValueError("Length mismatch")
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

        # Eval non-aggregated columns
        env = self.eval_env()
        non_agg = {}
        for alias, expr in columns.items():
            if alias in agg_ast:
                continue
            ast = other_ast.get(alias)
            if ast:
                arr = ast.eval(env=env)
            else:
                arr = self.columns[expr]

            if isinstance(arr, Alias):
                # un-pack alias
                arr, alias = arr.value, arr.name
            non_agg[alias] = arr

        # Early exit if we don't need to compute aggregates
        if not agg_ast:
            schema = Schema.from_frame(non_agg, idx_columns=list(non_agg))
            return Frame(schema, non_agg)

        res = {}
        if non_agg:
            # Compute binning
            records = rec.fromarrays(non_agg.values(), names=list(non_agg))
            keys, bins = unique(records, return_inverse=True)
            # Build resulting columns
            for alias in non_agg:
                arr = keys[alias]
                if isinstance(arr, Alias):
                    # un-pack alias
                    arr, alias = arr.value, arr.name
                res[alias] = arr
            env.update({"_keys": keys, "_bins": bins})

        # Compute aggregates
        for alias, expr in agg_ast.items():
            arr = expr.eval(env)
            if isinstance(arr, Alias):
                # un-pack alias
                arr, alias = arr.value, arr.name
            # Without bins, eval will return a scalar value
            res[alias] = arr if non_agg else asarray([arr])
        schema = Schema.from_frame(res, idx_columns=list(non_agg))
        return Frame(schema, res)

    def __getitem__(self, by):
        # By slice -> return a frame
        if isinstance(by, slice):
            start = by.start and self.schema.deserialize(by.start)
            stop = by.stop and self.schema.deserialize(by.stop)
            return self.islice(start, stop)
        # By mask -> return a frame
        if isinstance(by, ndarray):
            return self.mask(by)
        # By list -> return a frame with the corresponding columns
        if isinstance(by, list):
            cols = [self[c] for c in by]
            sch = Schema.from_frame(cols)
            return Frame(sch, cols)
        # By column name -> return an array
        if by in self.columns:
            return self.columns[by]
        else:
            raise KeyError(f'KeyError: "{by}"')

    def drop(self, *columns):
        keep_columns = (c for c in self.columns if c not in columns)
        return self.select(*keep_columns)

    def __iter__(self):
        return iter(self.columns)

    def select(self, keep):
        cols = {k: v for k, v in self.columns.items() if k in keep}
        return Frame(self.schema, cols)

    def rename(self, mapping):
        ...  # Use reduce instead ??

    def __repr__(self):
        res = []
        for name in self:
            res.append(name + "-> " + str(self[name]))
        return "\n".join(res)
