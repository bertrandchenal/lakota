from time import sleep

import numpy
from dask.distributed import Client, LocalCluster
from pandas import DataFrame, date_range

from lakota import POD, Frame, Repo, Schema
from lakota.utils import timeit

schema = Schema(timestamp="M8[s] *", value="int")
years = list(range(2000, 2020))


def insert(args):
    token, label, year = args
    pod = POD.from_token(token)
    repo = Repo(pod=pod)
    collection = repo / "my_collection"
    series = collection / label
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
    repo = Repo(pod=pod)
    # Create collection and label
    collection = repo.create_collection(schema, "my_collection")
    token = pod.token
    cluster = LocalCluster(processes=False)
    client = Client(cluster)
    args = [(token, label, y) for y in years]
    with timeit(f"\nWRITE ({pod.protocol})"):
        fut = client.map(insert, args)
        assert sum(client.gather(fut)) == 10_519_200
    client.close()
    cluster.close()

    # Merge everything and read series
    with timeit(f"\nMERGE ({pod.protocol})"):
        collection.merge()

    with timeit(f"\nREAD ({pod.protocol})"):
        series = collection / label
        df = series.df("2015-01-01", "2015-01-02")
        assert len(df) == 1440
        df = series.df("2015-12-31", "2016-01-02")
        assert len(df) == 2880


def do_defrag_and_gc(token):
    # We run defrag, trim & gc in parallel with the inserts
    pod = POD.from_token(token)
    repo = Repo(pod=pod)
    clc = repo.collection("my_collection")
    for i in range(10):
        clc.defrag(1)
        clc.trim()
        sleep(0.05)
    repo.gc()


def test_gc():
    # Create pod, repo & collection
    pod = POD.from_uri("memory://")
    token = pod.token
    label = "my_label"
    repo = Repo(pod=pod)
    clc = repo.create_collection(schema, "my_collection")
    # Start cluster & schedule concurrent writes & gc
    cluster = LocalCluster(processes=False)
    client = Client(cluster)
    args = [(token, label, y) for y in years]
    insert_fut = client.map(insert, args)
    gc_fut = client.submit(do_defrag_and_gc, token)
    assert sum(client.gather(insert_fut)) == 10_519_200
    client.gather(gc_fut)
    client.close()
    cluster.close()
    # Read data back
    clc.merge()
    frm = clc.series("my_label").frame()
    assert len(frm) == 10_519_200
