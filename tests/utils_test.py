from datetime import timedelta
from itertools import chain

import pytest

from lakota.utils import Closed, Pool, chunky, drange, strpt


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

    left = Closed.left
    right = Closed.right
    none = Closed.none
    both = Closed.both

    assert Closed["l"] is Closed.left
    assert Closed.r is Closed["RIGHT"]
    assert Closed.n is Closed.NONE
    assert Closed.both is Closed["b"]

    assert left.left_closed() and both.left_closed()
    assert not none.left_closed() and not right.left_closed()
    assert not left.right_closed() and both.right_closed()
    assert not none.right_closed() and right.right_closed()

    # Test set_left
    assert left.set_left(left) == left
    assert left.set_left(right) == none
    assert left.set_left(both) == left
    assert left.set_left(none) == none

    assert right.set_left(left) == both
    assert right.set_left(right) == right
    assert right.set_left(both) == both
    assert right.set_left(none) == right

    assert none.set_left(left) == left
    assert none.set_left(right) == none
    assert none.set_left(both) == left
    assert none.set_left(none) == none

    assert both.set_left(left) == both
    assert both.set_left(right) == right
    assert both.set_left(both) == both
    assert both.set_left(none) == right

    # Test set_right
    assert left.set_right(left) == left
    assert left.set_right(right) == both
    assert left.set_right(both) == both
    assert left.set_right(none) == left

    assert right.set_right(left) == none
    assert right.set_right(right) == right
    assert right.set_right(both) == right
    assert right.set_right(none) == none

    assert none.set_right(left) == none
    assert none.set_right(right) == right
    assert none.set_right(both) == right
    assert none.set_right(none) == none

    assert both.set_right(left) == left
    assert both.set_right(right) == both
    assert both.set_right(both) == both
    assert both.set_right(none) == left
