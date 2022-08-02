from lakota import Repo, Schema
from numpy import sin, cos, arange
from rich.progress import track

repo = Repo('.lakota')
schema = Schema(timestamp='timestamp*', value='float')
clc = repo.create_collection(schema, 'trigo', raise_if_exists=False)


with clc.multi():
    for name, fn in [('sin', sin), ('cos', cos)]:
        desc = f'Populate "{name}"'
        for i in track(range(0, 1_000_000_000, 1_000_000), desc):
            ts = arange(i, i + 1_000_000)
            srs = clc / name
            srs.write({
                'timestamp': ts,
                'value': fn(ts/50_000),
            })
