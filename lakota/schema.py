import shlex
from dataclasses import dataclass

from numcodecs import registry
from numpy import asarray, ascontiguousarray, dtype, frombuffer, issubdtype

DTYPES = [dtype(s) for s in ("M8[s]", "int64", "float64", "U", "O", "S20")]
ALIASES = {
    "timestamp": "M8[s]",
    "float": "f8",
    "int": "i8",
    "str": "U",
}
# TODO PRE-COMPUTED AGG: for each column in the index, compute
# aggregated values with that column removed


# TODO INDEXING pre-compute population per column (initialy per
# secondary index column, the ones that comes after the timestamp),
# this allows to speed up filters (and in some cases show the
# population itself). Ex pre-compute: meteor_id count. This can be
# saved along side the current digest in the revision payload


class Codec:
    def __init__(self, dt, *codec_names):
        # Make sure dtype is valid
        dt = dtype(ALIASES.get(dt, dt))
        for base_type in DTYPES:
            if issubdtype(dt, base_type):
                dt = base_type
                break
        else:
            raise ValueError(f"Column type '{dt}' not supported")
        self.dt = dt
        # Build list of codecs
        if codec_names:
            self.codec_names = codec_names
        else:
            # Adapt dtypes and codec_names
            default_codec_names = ["blosc"]
            if dt == dtype("<U"):
                default_codec_names = ["vlen-utf8", "zstd"]  # TODO use msgpck too
            elif dt == dtype("O"):
                default_codec_names = ["msgpack2", "zstd"]
            self.codec_names = default_codec_names

    def encode(self, arr):
        if len(arr) == 0:
            return b""
        # encoding may require contiguous memory
        arr = ascontiguousarray(arr)
        for codec_name in self.codec_names:
            codec = registry.codec_registry[codec_name]
            arr = codec().encode(arr)
        return arr

    def decode(self, arr):
        if len(arr) == 0:
            return asarray([], dtype=self.dt)
        for name in reversed(self.codec_names):
            codec = registry.codec_registry[name]
            arr = codec().decode(arr)
        if self.dt in ("<U", "O", "S"):
            return arr.astype(self.dt)
        return frombuffer(arr, dtype=self.dt)

    def __eq__(self, other):
        return self.codec_names == other.codec_names and self.dt == other.dt

    def __repr__(self):
        names = ", ".join(self.codec_names)
        return f"<Codec {self.dt}: {names}>"


class SchemaColumn:
    def __init__(self, name, dt, codecs, idx):
        self.name = name
        self.codec = Codec(dt, *codecs)
        self.idx = idx

    @classmethod
    def from_ui(cls, line):
        parser = shlex.shlex(line, posix=True, punctuation_chars="|*")
        parser.wordchars += "[]"
        name, dt, *tokens = parser
        idx = False
        codec_names = []
        state = None
        for tk in tokens:
            if tk == "|":
                state = "codec"
            elif tk == "*":
                idx = True
            elif state == "codec":
                codec_names.append(tk)
            else:
                raise ValueError(f"Unexpected item: {tk}")
        return SchemaColumn(name, dt, codecs=codec_names, idx=idx)

    def cast(self, arr):
        return asarray(arr, dtype=self.codec.dt)

    def dump(self):
        return {
            "dt": str(self.codec.dt),
            "codecs": self.codec.codec_names,
            "idx": self.idx,
        }


class Schema:
    def __init__(self, from_ui=None, from_columns=None, kind=None):
        assert (
            from_ui or from_columns
        ), "At least one of from_ui or from_columns is needed"
        assert kind in (None, "kv")
        self.kind = kind

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
                col = SchemaColumn.from_ui(line)
                self.columns[col.name] = col

        self.idx = {n: c for n, c in self.columns.items() if c.idx}
        self.non_idx = {n: c for n, c in self.columns.items() if not c.idx}
        if len(self.idx) == 0:
            raise ValueError(
                "Invalid schema, no index defined: " + str(from_ui or from_columns)
            )

    @classmethod
    def from_frame(cls, frame, idx_columns=None):
        """
        Instantiate a schema based on the column names and type if the given frame (a dict or a dataframe)
        """
        idx_columns = idx_columns or list(frame)
        col_defs = []
        for name in frame:
            arr = frame[name]
            col_defs.append(SchemaColumn(name, arr.dtype, [], name in idx_columns))
        return Schema(from_columns=col_defs)

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
        res = tuple(
            col.codec.dt.type(val) for col, val in zip(self.columns.values(), values)
        )
        return res

    @classmethod
    def loads(self, data):
        columns = [SchemaColumn(name, **opts) for name, opts in data["columns"].items()]
        return Schema(from_columns=columns, kind=data["kind"])

    def dump(self):
        columns = {c.name: c.dump() for c in self.columns.values()}
        return {"kind": self.kind, "columns": columns}

    def __iter__(self):
        return iter(self.columns.keys())

    def __repr__(self):
        cols = [f"{c.name} {c.codec.dt}" for c in self.columns.values()]
        return "<Schema {}>".format(" ".join(cols))

    def __eq__(self, other):
        return all(
            x == y for x, y in zip(self.columns.values(), other.columns.values())
        )

    def cast(self, df=None):
        if df is None:
            df = {}
            columns = list(self)
        else:
            columns = list(df)
        for name in columns:
            col = self[name]
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

    def __matmul__(self, labels):
        if not isinstance(labels, (list, tuple)):
            labels = [labels]
        return SeriesDefinition(self, labels)


@dataclass
class SeriesDefinition:
    schema: Schema
    labels: list
