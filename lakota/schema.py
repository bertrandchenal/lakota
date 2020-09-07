import shlex

from numcodecs import registry
from numpy import asarray, dtype, frombuffer

DTYPES = [dtype(s) for s in ("<M8[s]", "int64", "float64", "<U", "O")]
ALIASES = {
    "timestamp": "<M8[s]",
    "float": "float64",
    "int": "int64",
    "str": "<U",
}

# TODO AGGRAGTES: define column aggregate (avg, weighted avg, sum,
# count, min, max, first; last).  We can have simple rule to infer types of those (sum ->
# keep initial type, avg -> float, count -> int)

# TODO PRE-COMPUTED AGG: for each column in the index, compute
# aggregated values with that column removed

# XXX use argparse for col def ? like: "meteor_id int -a count -e blosc"

# TODO INDEXING pre-compute population per column (initialy per
# secondary index column, the ones that comes after the timestamp),
# this allows to speed up filters (and in some cases show the
# population itself). Ex pre-compute: meteor_id count. This can be
# saved along side the current digest in the revision payload


class ColumnDefinition:
    def __init__(self, name, dt, codecs, idx):
        self.name = name
        # Make sure dtype is valid
        dt = dtype(ALIASES.get(dt, dt))
        if dt not in DTYPES:
            raise ValueError("Column type '{dt}' not supported")
        self.dt = dt
        # Build list of codecs
        if codecs:
            self.codecs = codecs
        else:
            # Adapt dtypes and codecs
            default_codecs = ["blosc"]
            if dt == dtype("<U"):
                default_codecs = ["vlen-utf8", "gzip"]
            elif dt == dtype("O"):
                default_codecs = ["msgpack2", "zstd"]
            self.codecs = default_codecs
        # Is column part of the index:
        self.idx = idx

    def encode(self, arr):
        if len(arr) == 0:
            return b""
        for codec_name in self.codecs:
            codec = registry.codec_registry[codec_name]
            arr = codec().encode(arr)
        return arr

    def decode(self, arr):
        if len(arr) == 0:
            return asarray([], dtype=self.dt)
        for codec_name in reversed(self.codecs):
            codec = registry.codec_registry[codec_name]
            arr = codec().decode(arr)
        if self.dt in ("<U", "O"):
            return arr.astype(self.dt)
        return frombuffer(arr, dtype=self.dt)

    def cast(self, arr):
        return asarray(arr, dtype=self.dt)

    def __eq__(self, other):
        return self.name == other.name and self.dt == other.dt

    def __repr__(self):
        return f"<ColumnDefinition {self.name}:{self.dt}>"

    @classmethod
    def from_ui(cls, line):
        parser = shlex.shlex(line, posix=True, punctuation_chars="|*")
        parser.wordchars += "[]"
        name, dt, *tokens = parser
        kw = {"idx": False, "codecs": []}
        state = None
        for tk in tokens:
            if tk == "|":
                state = "codec"
            elif tk == "*":
                kw["idx"] = True
            elif state == "codec":
                kw["codecs"].append(tk)
            else:
                raise ValueError(f"Unexpected item: {tk}")
        return ColumnDefinition(name, dt, **kw)

    def dump(self):
        return {
            "name": self.name,
            "dt": str(self.dt),
            "codecs": self.codecs,
            "idx": self.idx,
        }


class Schema:
    def __init__(self, from_ui=None, from_columns=None):
        assert (
            from_ui or from_columns
        ), "At least one of from_ui or from_columns is needed"
        if from_columns:
            self.columns = {c.name: c for c in from_columns}
        else:
            self.columns = {}
            if not isinstance(from_ui, (list, tuple)):
                from_ui = from_ui.splitlines()
            for line in from_ui:
                line = line.strip()
                if not line:
                    continue
                col = ColumnDefinition.from_ui(line)
                self.columns[col.name] = col

        self.idx = {n: c for n, c in self.columns.items() if c.idx}
        if len(self.idx) == 0:
            raise ValueError(
                "Invalid schema, no index defined: " + str(from_ui or from_columns)
            )

    def serialize(self, values):
        if not values:
            return tuple()
        if not isinstance(values, (list, tuple)):
            values = (values,)
        # TODO implement column type based repr
        return tuple(str(val) for col, val in zip(self.columns.values(), values))

    def deserialize(self, values=tuple()):
        if not values:
            return tuple()
        if not isinstance(values, (list, tuple)):
            values = (values,)
        res = tuple(col.dt.type(val) for col, val in zip(self.columns.values(), values))
        return res

    @classmethod
    def loads(self, items):
        columns = [ColumnDefinition(**i) for i in items]
        return Schema(from_columns=columns)

    def dump(self):
        return [c.dump() for c in self.columns.values()]

    def __iter__(self):
        return iter(self.columns.keys())

    def __repr__(self):
        cols = [f"{c.name} {c.dt}" for c in self.columns.values()]
        return "<Schema {}>".format(" ".join(cols))

    def __eq__(self, other):
        return all(
            x == y for x, y in zip(self.columns.values(), other.columns.values())
        )

    def cast(self, df=None):
        df = {} if df is None else df
        for col in self.columns.values():
            df[col.name] = col.cast(df.get(col.name, []))
        return df

    def __getitem__(self, name):
        return self.columns[name]

    def row(self, df, pos, full=True):
        """
        Extract a row of the dataframe-like object at
        given position
        """
        cols = self.columns if full else self.idx
        return tuple(df[n][pos] for n in cols)
