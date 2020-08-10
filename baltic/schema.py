from numcodecs import registry
from numpy import array, dtype, frombuffer

DTYPES = [dtype(s) for s in ("<M8[s]", "int64", "float64", "<U", "O")]
ALIASES = {
    "timestamp": "<M8[s]",
    "float": "float64",
    "int": "int64",
}


class ColumnDefinition:
    def __init__(self, name, dt, codecs, idx):
        self.name = name
        # Make sure dtype is valid
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
        for codec_name in self.codecs:
            codec = registry.codec_registry[codec_name]
            arr = codec().encode(arr)
        return arr

    def decode(self, arr):
        for codec_name in reversed(self.codecs):
            codec = registry.codec_registry[codec_name]
            arr = codec().decode(arr)
        if self.dt in ("<U", "O"):
            return arr.astype(self.dt)
        return frombuffer(arr, dtype=self.dt)

    def __eq__(self, other):
        return self.name == other.name and self.dt == other.dt

    def __repr__(self):
        return f"<ColumnDefinition {self.name}:{self.dt}>"


class Schema:
    def __init__(self, columns, idx_len=0):
        self.columns = {}
        self.idx_len = idx_len or len(columns) - 1 or 1
        for pos, col in enumerate(columns):
            name, dt = col.split(":", 1)
            dt, *codecs = dt.split("|")
            dt = dtype(ALIASES.get(dt, dt))
            col = ColumnDefinition(name, dt, codecs, idx=pos < self.idx_len)
            self.columns[name] = col

        # All but last column is the default index
        self.idx = {n: c for n, c in self.columns.items() if c.idx}

    def serialize(self, values):
        if not values:
            return tuple()
        # TODO implement column type based repr
        return tuple(str(val) for col, val in zip(self.columns.values(), values))

    def deserialize(self, values=tuple()):
        if not values:
            return tuple()
        return tuple(
            col.dt.type(val) for col, val in zip(self.columns.values(), values)
        )

    @classmethod
    def loads(self, d):
        return Schema(columns=d["columns"], idx_len=d["idx_len"],)

    def as_dict(self):
        return {
            "columns": [f"{c.name}:{c.dt}" for c in self.columns.values()],
            "idx_len": len(self.idx),
            "fmt": "TODO CODEC",
        }

    def __repr__(self):
        cols = [f"{c.name}:{c.dt}" for c in self.columns.values()]
        return "<Schema {}>".format(" ".join(cols))

    def __eq__(self, other):
        return all(
            x == y for x, y in zip(self.columns.values(), other.columns.values())
        )

    def cast(self, df):
        for col in self.columns.values():
            df[col.name] = array(df[col.name]).astype(col.dt)
        return df

    def __getitem__(self, name):
        return self.columns[name]
