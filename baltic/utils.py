from time import perf_counter
from contextlib import contextmanager

@contextmanager
def timeit(title=''):
    start = perf_counter()
    yield
    delta = perf_counter() - start
    print(title, delta)


# def create_idx(self, name , arr):
#     keys, inv= unique(arr, return_inverse=True)
#     for pos, key in enumerate(keys):
#         idx = inv == pos
#         yield key, idx

# def read_idx(self, items):
#     res = None
#     for key, idx in items:
#         if res is None:
#             res = empty(shape=idx.shape, dtype='O')
#         res[idx] = key
#     return res
