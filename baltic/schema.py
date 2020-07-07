from numcodecs import registry
from numpy import array, dtype, frombuffer

DTYPES = [dtype(s) for s in ("<M8[s]", "int64", "float64", "<U", "O")]


class Schema:
    def __init__(self, columns, idx_len=0):
        self.columns = []
        self._dtype = {}
        self._codecs = {}
        for col in columns:
            name, dt = col.split(":", 1)
            dt, *codecs = dt.split("|")
            self.columns.append(name)
            # Make sure dtype is valid
            if dt not in DTYPES:
                raise ValueError("Column type '{dt}' not supported")

            # Adapt dtypes and codecs
            default_codecs = ["blosc"]
            if dt == dtype("<U"):
                default_codecs = ["vlen-utf8", "gzip"]
            elif dt == dtype("O"):
                default_codecs = ["json", "gzip"]

            dt = dtype(dt)
            self._dtype[name] = dt
            self._codecs[name] = codecs or default_codecs

        # All but last column is the default index
        idx_len = idx_len or len(columns) - 1 or 1
        self.idx = self.columns[:idx_len]

    def dtype(self, name):
        dt = self._dtype[name]
        return dt

    def codecs(self, name):
        return self._codecs[name]

    @classmethod
    def loads(self, d):
        return Schema(columns=d["columns"], idx_len=d["idx_len"],)

    def as_dict(self):
        return {
            "columns": [f"{c}:{self._dtype[c]}" for c in self.columns],
            "idx_len": len(self.idx),
            "fmt": "TODO CODEC",
        }

    def __repr__(self):
        cols = [f"{c}:{self._dtype[c]}" for c in self.columns]
        return "<Schema {}>".format(" ".join(cols))

    def __eq__(self, other):
        return all(
            (
                self.idx == other.idx,
                self.columns == other.columns,
                self._dtype == other._dtype,
            )
        )

    def serialize(self, values):
        if not values:
            return tuple()
        # TODO implement column type based repr
        return tuple(str(val) for col, val in zip(self.columns, values))

    def deserialize(self, values=tuple()):
        if not values:
            return tuple()
        return tuple(
            dtype(self.dtype(col)).type(val) for col, val in zip(self.columns, values)
        )

    def encode(self, name, arr):
        for codec_name in self.codecs(name):
            codec = registry.codec_registry[codec_name]
            arr = codec().encode(arr)
        return arr

    def decode(self, name, arr):
        dt = self.dtype(name)

        for codec_name in reversed(self.codecs(name)):
            codec = registry.codec_registry[codec_name]
            arr = codec().decode(arr)
        if self.dtype(name) in ("<U", "O"):
            return arr.astype(dt)
        return frombuffer(arr, dtype=dt)

    def cast(self, df):
        for name, col in df.items():
            dt = self.dtype(name)
            df[name] = array(col).astype(dt)
        return df
