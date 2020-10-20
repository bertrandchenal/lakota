import bisect
import logging
import sys
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha1
from itertools import islice
from pathlib import PosixPath
from time import perf_counter, time

from dateutil.relativedelta import relativedelta
from numpy import asarray

default_hash = sha1
head = lambda it, n=1: list(islice(it, 0, n))
tail = lambda it, n=1: deque(it, maxlen=n)
skip = lambda it, n: list(islice(it, n, None))
FLAGS = {}

fmt = "%(levelname)s:%(asctime).19s: %(message)s"
logging.basicConfig(format=fmt)
logger = logging.getLogger("lakota")
DEBUG = False


# Global settings
@dataclass
class Settings:
    threaded: bool


settings = Settings(threaded=False)


def chunky(collection, size=100):
    it = iter(collection)
    while True:
        chunk = head(it, size)
        if not chunk:
            break
        yield chunk


def hexdigest(*data):
    digest = default_hash()
    for datum in data:
        digest.update(datum)
    return digest.hexdigest()


def hextime(timestamp=None):
    """
    hex representation of current UTC time (rounded to millisecond)
    """
    timestamp = timestamp or time()
    return hex(int(timestamp * 1000))[2:]


def encoder(*items):
    "Auto-encode all items"
    for item in items:
        yield item.encode()


def strpt(time_str):
    if not time_str:
        return None
    return datetime.fromisoformat(time_str)


def drange(start, end, right_closed=False, **delta_args):
    start = strpt(start)
    end = strpt(end)
    res = []
    delta = relativedelta(**delta_args)
    while start <= end if right_closed else start < end:
        res.append(start)
        start += delta
    return asarray(res, dtype="M8[s]")


def hashed_path(digest, depth=2):
    """
    Pair-wise hashing of the `digest` string, example:
    12345678 -> (Path(12/34), "5678") (with depth = 2)
    """
    assert len(digest) > 2 * depth
    folder = PosixPath(".")
    for _ in range(depth):
        prefix, digest = digest[:2], digest[2:]
        folder = folder / prefix

    return folder, digest


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


def memoize(fn):
    fn = fn
    cache = {}

    def wrapper(*a, **kw):
        key = a + tuple(kw.keys()) + tuple(kw.values())
        if key in cache:
            return cache[key]
        res = fn(*a, **kw)
        cache[key] = res
        return res

    return wrapper


class Pool:
    """
    Threadpoolexecutor wrapper to simplify it's usage
    """

    def __init__(self):
        self.futures = []
        self.results = []
        self.pool = None

    def __enter__(self):
        if settings.threaded:
            self.pool = ThreadPoolExecutor(4)
        return self

    def submit(self, fn, *a, **kw):
        if settings.threaded:
            self.futures.append(self.pool.submit(fn, *a, **kw))
        else:
            self.results.append(fn(*a, **kw))

    def __exit__(self, type, value, traceback):
        if settings.threaded:
            self.results = [fut.result() for fut in self.futures]


def profile_object(*roots):
    """
    Usage:

    profiler = profile_object(SomeClass_or_some_object)
    ... run code
    profiler.print_stats()

    """
    # Monkey patch functions in module to add profiling decorator
    from inspect import isfunction

    import line_profiler

    profiler = line_profiler.LineProfiler()
    for root in roots:
        for key, item in root.__dict__.items():
            if isfunction(item):
                print(f"Enable profiler on {item.__name__} " f"in {root.__name__}")
                setattr(root, key, profiler(item))
    return profiler


def floor(arr, unit):
    """
    Floor the datetime array to the selected unit.
    unit can be 'Y', 'M', 'D', 'h', 'm' or 's'.
    """
    assert unit in "YMDhms"
    return arr.astype(f"M8[{unit}]")
