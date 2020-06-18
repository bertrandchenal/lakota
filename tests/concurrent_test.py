import numpy
from dask.distributed import Client, LocalCluster
from pandas import DataFrame, date_range

from baltic import Registry, Schema, Segment
from baltic.utils import timeit

schema = Schema(["timestamp:M8[s]", "value:int"])


def insert(args):
    registry, label, year = args
    series = registry.get(label)

    ts = date_range(f"{year}-01-01", f"{year+1}-01-01", freq="1min")
    df = DataFrame({"timestamp": ts,})

    df["value"] = numpy.round(numpy.random.random(len(ts)) * 1000, decimals=0)
    sgm = Segment.from_df(schema, df)
    series.write(sgm)
    return len(sgm)


def test_insert(pod):
    if pod.protocol == "memory":
        # Does not work with in-memory pod
        return

    # Write with workers
    label = "my_label"
    registry = Registry(pod=pod)
    registry.clear()  # FIXME shouldn't be needed
    registry.create(schema, label)

    cluster = LocalCluster(processes=False)
    client = Client(cluster)
    years = list(range(2000, 2020))
    args = [(registry, label, y) for y in years]
    with timeit("INSERT"):
        fut = client.map(insert, args)
        assert sum(client.gather(fut)) == 10_519_220
    client.close()
    cluster.close()

    # Read it back
    series = registry.get(label)
    df = series.read(["2015-01-01"], ["2015-01-02"]).df()
    assert len(df) == 1440
    df = series.read(["2015-12-31"], ["2016-01-02"]).df()
    assert len(df) == 2880
