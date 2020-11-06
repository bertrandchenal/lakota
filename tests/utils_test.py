from datetime import timedelta
from itertools import chain

import pytest

from lakota.utils import Pool, chunky, drange, strpt


def my_fun(i, flaky=False):
    if flaky and i == 2:
        raise ValueError("!")
    return i


def test_pool(threaded):

    # happy path
    with Pool() as pool:
        for i in range(3):
            pool.submit(my_fun, i)
    assert pool.results == [0, 1, 2]

    # unhappy
    with pytest.raises(ValueError):
        with Pool() as pool:
            for i in range(3):
                pool.submit(my_fun, i, flaky=True)


def test_chunk():
    for size in (1, 4, 13, 100):
        expected = list(range(size))
        chunks = chunky(expected)
        res = list(chain.from_iterable(chunks))
        assert res == expected


def test_drange():
    delta = timedelta(days=1)
    arr = drange("2020-01-01", "2020-01-10", delta)
    assert len(arr) == 9
    assert arr[0] == strpt("2020-01-01")
    assert arr[-1] == strpt("2020-01-09")

    delta = timedelta(days=20)
    arr = drange("2020-01-01", "2020-01-10", delta)
    assert len(arr) == 1
    assert arr[0] == strpt("2020-01-01")
