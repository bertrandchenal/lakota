from numpy.random import random
from pandas import DataFrame, date_range

from lakota import Repo, Schema
from lakota.utils import hextime

suffix = hextime()

SIZE = 100_000
values = random(SIZE)
timestamps = date_range("1970-01-01", freq="5min", periods=SIZE)
df = DataFrame(
    {
        "ts": timestamps,
        "value": values,
    }
)

df.to_csv(f"timeseries-{suffix}.csv")
df.to_parquet(f"timeseries-{suffix}.pqt")


repo = Repo("repo")
schema = Schema(ts="timestamp*", value="float")
clct = repo / "my_collection"
if not clct:
    clct = repo.create_collection(schema, "my_collection")
series = clct / "my_series"
series.write(df)


# Results

# $ python examples/data_size.py
# $ du -hs timeseries-* repo
# 4.4M    timeseries-178592d4827.csv
# 3.3M    timeseries-178592d4827.pqt
# 712K    repo
