import os
import shutil

from dask.distributed import Client, LocalCluster
from numpy.random import random
from pandas import DataFrame, date_range

from baltic import Registry, Schema, Segment


def insert(args):
    series, schema, year = args
    ts = date_range(f'{year}-01-01', f'{year+1}-01-01', freq='1min')
    df = DataFrame({
        'timestamp': ts,
    })

    df['value'] = random(len(ts))
    sgm = Segment.from_df(schema, df)
    series.write(sgm)
    return year

def test_insert():
    schema = Schema(['timestamp:M8[s]', 'value:f'])
    test_dir = '/dev/shm/test_data'
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    registry = Registry(test_dir)
    registry.create(schema, 'my_label')
    series = registry.get('my_label')
    years = list(range(2000, 2020))

    cluster = LocalCluster()
    client = Client(cluster)
    args = [(series, schema, y) for y in years]
    fut = client.map(insert, args)
    client.gather(fut)

    df = series.read(['2015-01-01'], ['2015-01-02']).df()
    assert len(df) == 1440

    df = series.read(['2015-12-31'], ['2016-01-02']).df()
    assert len(df) == 2880

