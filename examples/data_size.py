from numpy.random import random
from pandas import DataFrame, date_range

from lakota import Repo, Schema
from lakota.utils import hextime, timeit

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
df.to_parquet(f"timeseries-{suffix}.snappy.pqt", compression='snappy')
df.to_parquet(f"timeseries-{suffix}.brotli.pqt", compression='brotli')
with timeit('pqt'):
    df.to_parquet(f"timeseries-{suffix}.gzip.pqt", compression='gzip')


repo = Repo("repo")
schema = Schema(ts="timestamp*", value="float")
clct = repo / "my_collection"
if not clct:
    clct = repo.create_collection(schema, "my_collection")
series = clct / "my_series"

with timeit('lk'):
    series.write(df)


## Results

# $ python examples/data_size.py
# pqt 198.76ms
# lk 24.24ms


# $ du -hs timeseries-* repo
# 1,4M	timeseries-17a813a84a1.brotli.pqt
# 4,4M	timeseries-17a813a84a1.csv
# 1,5M	timeseries-17a813a84a1.gzip.pqt
# 1,8M	timeseries-17a813a84a1.snappy.pqt
# 732K	repo

## And with gzip compression of csv
# $ gzip timeseries-17a813a84a1.csv
# $ du -hs timeseries-17a813a84a1.csv.gz
# 1,5M	timeseries-17a813a84a1.csv.gz
