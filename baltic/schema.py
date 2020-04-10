import json


class Schema:

    def __init__(self, dimensions, measures):
        self.dimensions = dimensions
        self.measures = measures

    @classmethod
    def loads(self, content):
        d = json.loads(content)
        return Schema(
            dimensions=d['dimensions'],
            measures=d['measures'],
        )

    def dumps(self):
        return json.dumps({
            'dimensions': self.dimensions,
            'measures': self.measures,
        })
