import os
from bisect import bisect
from numpy import arange, searchsorted
import zarr
from time import perf_counter
from contextlib import contextmanager

@contextmanager
def timeit(title=''):
    start = perf_counter()
    yield
    delta = perf_counter() - start
    print(title, delta)


def load():
    store = 'bisect_test_store.zarr'
    if os.exists(store):
        return
    step = 500_000
    z = zarr.open(store, mode='w', shape=(0,),
                  chunks=(10_000), dtype='i8')

    for i in range(0, 100 * step, step):
        a = arange(i, i + step)
        with timeit(f'save {i}'):
            z.append(a)

def timings():
    # z = da.from_zarr('example.zarr')
    z = zarr.open('example.zarr')

    with timeit('item'):
        v = z[500]

    with timeit('bisect'):
        assert 5000001 == bisect(z, 5000000)

    with timeit('searchsorted'):
        assert 5000000 == searchsorted(z, 5000000)

#load()
timings()
