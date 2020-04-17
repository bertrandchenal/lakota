import json

from .utils import head

class Schema:

    def __init__(self, columns, index=None):
        for name, type_ in columns.items():
            if type_ in ('dim', 'msr'):
                continue
            msg = f'Column type {type_} for column {name} not supported'
            raise ValueError(msg)

        # First column is the default index
        if index is None:
            index = head(columns, 1)
        for name in index:
            if name in columns:
                continue
            msg = f'Undefined column "{name}" in index'
            raise ValueError(msg)
        self.columns = columns
        self.index = index

    @classmethod
    def loads(self, content):
        d = json.loads(content)
        return Schema(
            columns=d['columns'],
            index=d['index'],
        )

    def dumps(self):
        return json.dumps({
            'columns': self.columns,
            'index': self.index,
        })
