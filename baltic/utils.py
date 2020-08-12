import bisect
import logging
import sys
from collections import deque
from contextlib import contextmanager
from datetime import date, datetime
from hashlib import sha1
from itertools import islice
from pathlib import PosixPath
from time import perf_counter, time

default_hash = sha1
head = lambda it, n=1: list(islice(it, 0, n))
tail = lambda it, n=1: deque(it, maxlen=n)
skip = lambda it, n: list(islice(it, n, None))
FLAGS = {}

fmt = "%(levelname)s:%(asctime).19s: %(message)s"
logging.basicConfig(format=fmt)
logger = logging.getLogger("baltic")
DEBUG = False


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


def strpt(time_str):
    if isinstance(time_str, datetime):
        return time_str
    elif isinstance(time_str, date):
        return datetime(time_str.year, time_str.month, time_str.day)

    candidates = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
    ]
    for fmt in candidates:
        try:
            return datetime.strptime(time_str, fmt)
        except ValueError:
            pass
    raise ValueError('Unable to parse "%s" as datetime' % time_str)


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


class MemoizeWrapper:
    def __init__(self, fn):
        self.fn = fn
        self.cache = {}

    def __call__(self, *args):
        if args in self.cache:
            return self.cache[args]
        res = self.fn(*args)
        self.cache[args] = res
        return res

    def clear(self):
        self.cache = {}


def memoize(fn):
    return MemoizeWrapper(fn)
