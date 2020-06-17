import json

from numpy import dtype, frombuffer
from numcodecs import Blosc, VLenUTF8


class Schema:

    def __init__(self, columns, idx_len=0):
        self.columns = []
        self._dtype = {}
        for col in columns:
            name, dt = col.split(':', 1)
            self.columns.append(name)
            # Make sure dtype is valid
            dtype(dt)
            self._dtype[name] = dt

        # All but last column is the default index
        idx_len = idx_len or len(columns) - 1
        self.idx = self.columns[:idx_len]

    def dtype(self, name):
        return self._dtype[name]

    @classmethod
    def loads(self, content):
        d = json.loads(content)
        return Schema(
            columns=d['columns'],
            idx_len=d['idx_len'],
        )

    def dumps(self):
        return json.dumps({
            'columns': [f'{c}:{self._dtype[c]}' for c in self.columns],
            'idx_len': len(self.idx),
            'fmt': 'TODO CODEC',
        })

    def __repr__(self):
        cols = [f'{c}:{self._dtype[c]}' for c in self.columns]
        return '<Schema {}>'.format(' '.join(cols))

    def __eq__(self, other):
        return all((
            self.idx == other.idx,
            self.columns == other.columns,
            self._dtype == other._dtype,
        ))

    def serialize(self, values):
        res = []
        for col, val in zip(self.columns, values):
            res.append(str(val))
        return res

    def deserialize(self, values):
        res = []
        for col, val in zip(self.columns, values):
            val = dtype(self.dtype(col)).type(val)
            res.append(val)
        return res

    def encode(self, name, arr):
        dt = self.dtype(name)
        codec = VLenUTF8() if dt == 'str' else Blosc()
        return codec.encode(arr)

    def decode(self, name, data):
        dt = self.dtype(name)
        if dt == 'str':
            try:
                res = VLenUTF8().decode(data)
            except:
                import pdb;pdb.set_trace()
            return res
        data = Blosc().decode(data)
        return frombuffer(data, dtype=dt)
