import shlex
from dataclasses import dataclass

from numcodecs import registry
from numpy import asarray, ascontiguousarray, dtype, frombuffer, issubdtype, ndarray

DTYPES = [dtype(s) for s in ("datetime64[s]", "int64", "float64", "U", "O")]

ALIASES = {
    "timestamp": "M8[s]",
    "float": "f8",
    "int": "i8",
    "str": "U",
}

__all__ = ["Schema"]


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
            if dt in (dtype("O"), dtype("U")):
                default_codec_names = ["msgpack2", "zstd"]
            self.codec_names = default_codec_names

    def encode(self, arr):
        if len(arr) == 0:
            return b""
        # encoding may require contiguous memory
        arr = ascontiguousarray(arr)
        # convert to proper type
        arr = arr.astype(self.dt)
        # Apply codecs
        for codec_name in self.codec_names:
            codec = registry.codec_registry[codec_name]
            kw = {}
            if codec_name == "blosc":
                kw = {
                    "cname": "zstd",
                    "shuffle": codec.BITSHUFFLE,
                }
            arr = codec(**kw).encode(arr)
        return arr

    def decode(self, arr):
        if len(arr) == 0:
            return asarray([], dtype=self.dt)
        # Apply all codecs
        for name in reversed(self.codec_names):
            codec = registry.codec_registry[name]
            arr = codec().decode(arr)
        if self.dt in ("O", "U"):
            return arr.astype(self.dt)
        return frombuffer(arr, dtype=self.dt)

    def __eq__(self, other):
        return self.codec_names == other.codec_names and self.dt == other.dt

    def __repr__(self):
        names = ", ".join(self.codec_names)
        return f"<Codec {self.dt}:{names}>"


class SchemaColumn:
    def __init__(self, name, dt, codecs, idx):
        self.name = name
        self.codec = Codec(dt, *codecs)
        self.idx = idx

    @classmethod
    def from_ui(cls, name, definition):
        parser = shlex.shlex(definition, posix=True, punctuation_chars="|*")
        parser.wordchars += "[]"
        dt, *tokens = parser
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
        if isinstance(arr, ndarray) and issubdtype(arr.dtype, self.codec.dt):
            return arr
        return asarray(arr, dtype=self.codec.dt)

    def cast_scalar(self, value):
        return dtype(self.codec.dt).type(value)

    def dumps(self):
        return {
            "dt": str(self.codec.dt),
            "codecs": self.codec.codec_names,
            "idx": self.idx,
        }

    def __eq__(self, other):
        return (
            self.name == other.name
            and self.idx == other.idx
            and self.codec == other.codec
        )


class Schema:
    def __init__(self, **columns):
        self.kind = None
        self.columns = {}
        for name, definition in columns.items():
            if not isinstance(definition, SchemaColumn):
                definition = SchemaColumn.from_ui(name, definition)
            self.columns[name] = definition

        self.idx = {n: c for n, c in self.columns.items() if c.idx}
        self.non_idx = {n: c for n, c in self.columns.items() if not c.idx}
        if len(self.idx) == 0:
            raise ValueError("Invalid schema, no index defined")

    @classmethod
    def kv(cls, **columns):
        schema = Schema(**columns)
        schema.kind = "kv"
        return schema

    @classmethod
    def from_frame(cls, frame, idx_columns=None):
        """
        Instantiate a schema based on the column names and type if the given frame (a dict or a dataframe)
        """
        idx_columns = idx_columns or list(frame)
        col_defs = {}
        for name in frame:
            arr = frame[name]
            col_defs[name] = SchemaColumn(name, arr.dtype, [], name in idx_columns)
        return Schema(**col_defs)

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
            col.cast_scalar(val) for col, val in zip(self.columns.values(), values)
        )
        return res

    @classmethod
    def loads(self, data):
        columns = {
            name: SchemaColumn(name, **opts) for name, opts in data["columns"].items()
        }
        if data["kind"] == "kv":
            return Schema.kv(**columns)
        return Schema(**columns)

    def dumps(self):
        columns = {c.name: c.dumps() for c in self.columns.values()}
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
            columns = [c for c in df if c in self]

        res = {}
        for name in columns:
            col = self[name]
            res[col.name] = col.cast(df.get(col.name, []))
        return res

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
