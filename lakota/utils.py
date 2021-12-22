import bisect
import logging
import sys
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Flag
from hashlib import sha1
from itertools import islice
from pathlib import PurePosixPath
from threading import Lock
from time import perf_counter, time

import pytz
from numpy import arange, array, minimum, searchsorted

default_hash = sha1
hexhash_len = 40
head = lambda it, n=1: list(islice(it, 0, n))
tail = lambda it, n=1: deque(it, maxlen=n)
skip = lambda it, n: list(islice(it, n, None))
FLAGS = {}

fmt = "%(levelname)s:%(asctime).19s: %(message)s"
logging.basicConfig(format=fmt)
logger = logging.getLogger("lakota")


# Global settings
@dataclass
class Settings:
    threaded: bool = True
    debug: bool = False
    verify_ssl: bool = True
    embed_max_size: int = 1024
    page_len: int = 500_000
    defrag_max_chunk: int = 4  # Max number of small chunks in a series
    timeout: int = 600  # Max duration for a write batch (in seconds)
    max_threads: int = 10 # Size of the threadpool

settings = Settings()


def pivot(records, keys):
    """
    Convert a list of dict to a dict of lists
    """
    if not records:
        return {}
    # Extract column names from first record
    keys = list(records[0])
    columns = [[i[k] for i in records] for k in keys]
    return dict(zip(keys, columns))


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

    >>> from lakota.utils import hextime
    >>> hextime()
    '17b7d8a8691'
    >>>
    """
    timestamp = time() if timestamp is None else timestamp
    hex_ts = hex(
        int(timestamp * 1000)
    )  # get rid of sub-milisecond digits & convert fo hex
    return hex_ts[2:].rjust(11, "0")  # remove the "0x" prefix and add zeroes on front


def encoder(*items):
    "Auto-encode all items"
    for item in items:
        yield item.encode()


def strpt(time_str):
    if isinstance(time_str, datetime):
        return time_str
    if not time_str:
        return None
    return datetime.fromisoformat(time_str)


def drange(start, end, delta, right_closed=False):
    start = strpt(start)
    end = strpt(end)
    return arange(start, end, delta).astype("M8[s]")


def paginate(start, stop, **delta_kw):
    step = start
    delta = timedelta(**delta_kw)
    assert delta.total_seconds() > 0, "Delta of zero length!"
    while True:
        next_step = step + delta
        yield step, min(next_step, stop)
        if next_step >= stop:
            break
        step = next_step


def hashed_path(digest, depth=2):
    """
    Pair-wise hashing of the `digest` string, example:
    12345678 -> (Path(12/34), "5678") (with depth = 2)
    """
    assert len(digest) > 2 * depth
    folder = PurePosixPath(".")
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
    Threadpoolexecutor wrapper to simplify its usage
    """

    _lock = Lock()
    _pool = None

    def __init__(self):
        self.threaded = False
        self.futures = []
        self.results = []

    def __enter__(self):
        if not settings.threaded:
            return self
        # Get lock and update self.threaded
        locked = Pool._lock.acquire(blocking=False)
        self.threaded = locked
        return self

    def submit(self, fn, *a, **kw):
        if self._pool is None:
            self._pool = ThreadPoolExecutor(settings.max_threads)
        if self.threaded:
            self.futures.append(self._pool.submit(fn, *a, **kw))
        else:
            self.results.append(fn(*a, **kw))

    def __exit__(self, type, value, traceback):
        if self.threaded:
            self.results = [fut.result() for fut in self.futures]
        # Release lock
        if self.threaded:
            Pool._lock.release()
            self.threaded = False


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
    if unit == "W":
        ...  # TODO
    assert unit in "YMDhms"
    return arr.astype(f"M8[{unit}]")


def day_of_week_num(arr):
    # see https://stackoverflow.com/a/54264187: "takes advantage of
    # the fact that numpy.datetime64s are relative to the unix epoch,
    # which was a Thursday."
    return (arr.astype("M8[D]").view("int64") - 4) % 7


_tz_cache = {}


def as_tz(arr, timezone):
    """
    Convert UTC-naive array of timestamps to `timezone` (still naive)
    """
    if timezone in _tz_cache:
        transitions, offsets = _tz_cache[timezone]
    else:
        tz = pytz.timezone(timezone)
        transitions = array(tz._utc_transition_times, dtype="M8[s]")
        offsets = array([i[0].total_seconds() for i in tz._transition_info], dtype="i8")
        _tz_cache[timezone] = transitions, offsets

    # Compute mapping between transition time and offsets
    mapping = searchsorted(transitions, arr, side="right") + 1
    # Fallback to last offset when last transition os reached
    mapping = minimum(mapping, len(offsets) - 1)
    # Apply mapping
    return arr.astype("M8[s]") + offsets[mapping]


def yaml_load(stream):
    import yaml

    class OrderedLoader(yaml.SafeLoader):
        pass

    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return dict(loader.construct_pairs(node))

    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, construct_mapping
    )
    return yaml.load(stream, OrderedLoader)


class Interval:
    labels = ["m", "h", "D", "W", "M", "Y", None]
    durations = [
        60,  # a minute
        3600,  # h: 60*60
        86_400,  # D: 3600 * 24
        604_800,  # W: 604800 * 7
        2_592_000,  # M: 604800 * 30
        31_536_000,  # Y: 604800 * 365
    ]

    @classmethod
    def bisect(cls, nb_seconds):
        idx = bisect.bisect_right(cls.durations, nb_seconds)
        label = cls.labels[idx]
        return label


class Closed(Flag):
    NONE = n = 0  # 00
    RIGHT = r = 1  # 01
    LEFT = l = 2  # 10
    BOTH = b = 3  # 11

    @classmethod
    def cast(cls, value):
        if isinstance(value, cls):
            return value
        return Closed[value]

    @property
    def short(self):
        return self.name[0].lower()

    @property
    def left(self):
        return bool(self & Closed.LEFT)

    @property
    def right(self):
        return bool(self & Closed.RIGHT)

    def set_left(self, value):
        return self | Closed.LEFT if value else self & Closed.RIGHT

    def set_right(self, value):
        return self | Closed.RIGHT if value else self & Closed.LEFT
