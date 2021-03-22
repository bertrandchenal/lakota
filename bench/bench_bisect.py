# Script output:

# item 0.011694499757140875
# bisect 0.018132199998944998
# searchsorted 1.1717986003495753


import os
from bisect import bisect
from contextlib import contextmanager
from time import perf_counter

import zarr
from numpy import arange, searchsorted


@contextmanager
def timeit(title=""):
    start = perf_counter()
    yield
    delta = perf_counter() - start
    print(title, delta)


store = "bisect_test_store.zarr"


def load():
    if os.path.exists(store):
        return
    step = 500_000
    z = zarr.open(store, mode="w", shape=(0,), chunks=(10_000), dtype="i8")

    for i in range(0, 100 * step, step):
        a = arange(i, i + step)
        with timeit(f"save {i}"):
            z.append(a)


def timings():
    # z = da.from_zarr('example.zarr')
    z = zarr.open(store)

    with timeit("item"):
        _ = z[500]

    with timeit("bisect"):
        assert 5000001 == bisect(z, 5000000)

    with timeit("searchsorted"):
        assert 5000000 == searchsorted(z, 5000000)


load()
timings()
