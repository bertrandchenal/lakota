import numpy
from dask.distributed import Client, LocalCluster
from pandas import DataFrame, date_range

from jensen import POD, Frame, Registry, Schema
from jensen.utils import timeit

schema = Schema(["timestamp M8[s] *", "value int"])


def insert(args):
    token, label, year = args
    pod = POD.from_token(token)
    registry = Registry(pod=pod)
    series = registry.get(label)
    ts = date_range(f"{year}-01-01", f"{year+1}-01-01", freq="1min", closed="left")
    df = DataFrame(
        {
            "timestamp": ts,
            "value": numpy.round(numpy.random.random(len(ts)) * 1000, decimals=0),
        }
    )

    sgm = Frame(schema, df)
    series.write(sgm)
    return len(sgm)


def test_insert(pod):
    # Write with workers
    label = "my_label"
    registry = Registry(pod=pod)
    token = pod.token
    registry.create(schema, label)

    cluster = LocalCluster(processes=False)
    client = Client(cluster)
    years = list(range(2000, 2020))
    args = [(token, label, y) for y in years]
    with timeit(f"\nWRITE ({pod.protocol})"):
        fut = client.map(insert, args)
        assert sum(client.gather(fut)) == 10_519_200
    client.close()
    cluster.close()

    # Read it back
    with timeit(f"\nREAD ({pod.protocol})"):
        series = registry.get(label)
        df = series.closed("left")["2015-01-01":"2015-01-02"].df()
        assert len(df) == 1440
        df = series.closed("left")["2015-12-31":"2016-01-02"].df()
        assert len(df) == 2880
