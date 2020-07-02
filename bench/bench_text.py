from random import randint
import zarr
from uuid import uuid4
from time import perf_counter
from contextlib import contextmanager

from numcodecs import Blosc
compressor = Blosc(cname='snappy')

@contextmanager
def timeit(title=''):
    start = perf_counter()
    yield
    delta = perf_counter() - start
    print(title, delta)


def data(i):
    return [
        'abc'*30,
        'def'*30,
        'ijk'*30
    ][i%3]


N =100_000
data = [data(i) for i in range(N)]
gr = zarr.group()
arr = zarr.array(data, dtype=str)
# gr['names'] = arr


st = zarr.open('text_store.zarr')
with timeit('store'):
    zarr.copy(arr, st, 'names')

with open('text_file.txt', 'w') as fh:
    fh.write('\n'.join(data))

print(st['names'][101] == data[101])
