import pytest

from lakota.utils import Pool


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
