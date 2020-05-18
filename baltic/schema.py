import json

DTYPES = [
    'S32',
    'float',
    'int',
    'str',
]

class Schema:

    def __init__(self, columns, idx_len=0):
        self.columns = []
        self._dtype = {}
        for col in columns:
            name, dtype = col.split(':', 1)
            assert dtype in DTYPES
            self.columns.append(name)
            self._dtype[name] = dtype

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
            'fmt': 'segment.zarr.v1',
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
