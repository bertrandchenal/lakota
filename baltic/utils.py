import bisect
import sys
from collections import deque
from contextlib import contextmanager
from hashlib import sha1
from itertools import islice
from time import perf_counter

default_hash = sha1
hexdigest = lambda data: sha1(data).hexdigest()
head = lambda it, n: list(islice(it, 0, n))
tail = lambda it, n: deque(it, maxlen=n)
skip = lambda it, n: list(islice(it, n, None))


def pretty_nb(number):
    prefixes = "yzafpnum_kMGTPEZY"
    factors = [1000 ** i for i in range(-8, 8)]
    if number == 0:
        return 0
    if number < 0:
        return "-" + pretty_nb(-number)
    idx = bisect.bisect_right(factors, number) - 1
    prefix = prefixes[idx]
    return "%.2f%s" % (number / factors[idx], "" if prefix == "_" else prefix)


@contextmanager
def timeit(title=""):
    start = perf_counter()
    yield
    delta = perf_counter() - start
    print(title, pretty_nb(delta) + "s", file=sys.stderr)


# def read_schema(schema_string):
#     omg = OmegaConf.create(schema_string)
#     # TODO validation
#     return omg

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
