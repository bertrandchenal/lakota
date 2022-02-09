"""
# Lakota

Lakota is a columnar storage solution for timeseries.

Lakota organises reads and writes through a changelog inspired by
Git. This changelog provides: historisation, concurrency control and
ease of synchronisation across different storage backends.


## Quickstart

Install with `pip install lakota`

You can then run:

``` python
from lakota import Repo, Schema

ts_schema = Schema(timestamp="timestamp*", value="float")
repo = Repo("my-data-folder")  # or Repo("s3://my-s3-bucket")
clct = repo.create_collection(ts_schema, "temperature")
series = clct.series('Brussels')
df = {
    "timestamp": [
        "2020-01-01T00:00",
        "2020-01-02T00:00",
        "2020-01-03T00:00",
        "2020-01-04T00:00",
    ],
    "value": [1, 2, 3, 4],
}

series.write(df)
df = series[:'2020-01-03'].df()
print(df)
# ->
#    timestamp  value
# 0 2020-01-01    1.0
# 1 2020-01-02    2.0
# 2 2020-01-03    3.0
```

See `lakota.repo` and `lakota.schema` on how to create a repository
and define collections.

See `lakota.collection` and `lakota.series` on how to create series
and read/write data.

`lakota.commit` and `lakota.changelog` document how data is organised
into files and directories.
"""

from .changelog import *
from .collection import *
from .frame import *
from .pod import *
from .repo import *
from .schema import *
from .series import *

__version__ = "0.6.7"
