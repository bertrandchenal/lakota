from datetime import timedelta
from itertools import chain

import pytest
from pandas import date_range

from lakota.utils import Closed, Pool, as_tz, chunky, drange, strpt, timeit


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


def test_closed():

    left = Closed.LEFT
    right = Closed.RIGHT
    none = Closed.NONE
    both = Closed.BOTH

    assert Closed["l"] is Closed.LEFT
    assert Closed.r is Closed["RIGHT"]
    assert Closed.n is Closed.NONE
    assert Closed.BOTH is Closed["b"]

    assert left.left and both.left
    assert not none.left and not right.left
    assert not left.right and both.right
    assert not none.right and right.right

    # Test set_left
    assert left.set_left(True) == left
    assert left.set_left(False) == none

    assert right.set_left(True) == both
    assert right.set_left(False) == right

    assert none.set_left(True) == left
    assert none.set_left(False) == none

    assert both.set_left(True) == both
    assert both.set_left(False) == right

    # Test set_right
    assert left.set_right(False) == left
    assert left.set_right(True) == both

    assert right.set_right(False) == none
    assert right.set_right(True) == right

    assert none.set_right(False) == none
    assert none.set_right(True) == right

    assert both.set_right(False) == left
    assert both.set_right(True) == both


@pytest.mark.parametrize("tzname", ["US/Pacific", "Europe/Brussels", "Japan"])
def test_as_tz(tzname):
    ts = date_range("2000-01-01", "2020-01-01", freq="H")
    expected = ts.tz_localize("UTC").tz_convert(tzname).tz_localize(None)
    res = as_tz(ts.values, tzname)
    assert all(res == expected.values.astype("M8[s]"))
