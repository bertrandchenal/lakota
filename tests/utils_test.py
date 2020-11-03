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
    arr = drange("2020-01-01", "2020-01-10", days=1)
    assert len(arr) == 9
    assert arr[0] == strpt("2020-01-01")
    assert arr[-1] == strpt("2020-01-09")

    arr = drange("2020-01-01", "2020-01-10", right_closed=True, days=1)
    assert len(arr) == 10
    assert arr[-1] == strpt("2020-01-10")

    arr = drange("2020-01-01", "2020-01-10", days=20)
    assert len(arr) == 1
    assert arr[0] == strpt("2020-01-01")

    arr = drange("2020-01-01", "2020-12-01", months=1)
    assert len(arr) == 11
    assert arr[1] == strpt("2020-02-01")
    assert arr[-1] == strpt("2020-11-01")
