import bisect
import logging
import sys
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from hashlib import sha1
from itertools import islice
from pathlib import PosixPath
from time import perf_counter, time

from numpy import arange

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
    if isinstance(time_str, datetime):
        return time_str
    elif isinstance(time_str, date):
        return datetime(time_str.year, time_str.month, time_str.day)

    candidates = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in candidates:
        try:
            return datetime.strptime(time_str, fmt)
        except ValueError:
            pass
    raise ValueError('Unable to parse "%s" as datetime' % time_str)


def drange(start, end, **time_delta_args):
    start = strpt(start)
    end = strpt(end)
    return arange(start, end, timedelta(**time_delta_args))


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
