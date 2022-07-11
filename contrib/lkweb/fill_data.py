from lakota import Repo, Schema
from numpy import sin, cos, arange

repo = Repo('.lakota')
schema = Schema(timestamp='timestamp*', value='float')
clc = repo.create_collection(schema, 'trigo', raise_if_exists=False)

# clc = repo / 'trigo'

with clc.multi():
    for i in range(0, 1_000_000_000, 1_000_000):
        print(i)
        ts = arange(i, i + 1_000_000)
        srs = clc / 'cos'
        srs.write({
            'timestamp': ts,
            'value': cos(ts/50_000),
        })

        srs = clc / 'sin'
        srs.write({
            'timestamp': ts,
            'value': sin(ts/50_000),
        })
