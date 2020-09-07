import pytest
from numpy import array

from lakota import Frame, Registry, Schema

NAMES = list("abcde")
VALUES = [1.1, 2.2, 3.3, 4.4, 5.5]


@pytest.fixture
def frame_values():
    return {
        "value": VALUES,
        "category": NAMES,
    }


@pytest.fixture
def frm(frame_values):
    schema = Schema(["category str*", "value float"])

    frm = Frame(schema, frame_values)
    return frm


def test_index_slice():
    schema = Schema(["x int*"])
    frm = Frame(schema, {"x": [1, 2, 3, 4, 5, 5, 5, 6]})

    # include both side
    res = frm.index_slice([2], [4], closed="both")["x"]
    assert all(res == [2, 3, 4])

    # include only left
    res = frm.index_slice([2], [4], closed="left")["x"]
    assert all(res == [2, 3])

    # include only right
    res = frm.index_slice([2], [4], closed="right")["x"]
    assert all(res == [3, 4])

    # implict right
    res = frm.index_slice([5], [5], closed="both")["x"]
    assert all(res == [5, 5, 5])

    res = frm.index_slice([1], [1], closed="both")["x"]
    assert all(res == [1])

    res = frm.index_slice([6], [6], closed="both")["x"]
    assert all(res == [6])


def test_getitem():
    # with a slice
    schema = Schema(["x int*"])
    frm = Frame(schema, {"x": [1, 2, 3, 4, 5, 5, 5, 6]})
    frm2 = frm[5:]
    assert all(frm2["x"] == [5, 5, 5, 6])

    # with a mask
    frm2 = frm[array([True, False] * 4)]
    assert all(frm2["x"] == [1, 3, 5, 5])


def test_mask():
    # with an array
    schema = Schema(["x int*"])
    frm = Frame(schema, {"x": [1, 2, 3, 4, 5, 5, 5, 6]})
    frm2 = frm.mask(array([True, False] * 4))
    assert all(frm2["x"] == [1, 3, 5, 5])

    # with an expression
    frm2 = frm.mask("x % 2 == 0")
    assert all(frm2["x"] == [2, 4, 6])


def test_double_slice(frame_values, frm):
    # in-meory frame
    frm = frm.slice(1, None).slice(None, 2)
    assert all(frm["value"] == VALUES[1:][:2])

    # frame created from registry
    reg = Registry()
    series = reg.create(frm.schema, "my-label")
    series.write(frame_values)
    frm = series.frame()
    frm = frm.slice(1, None).slice(None, 2)
    assert all(frm["value"] == VALUES[1:][:2])


def test_reduce(frm):
    # Basic frame with only one index columns
    frm = frm.drop("category").reduce()
    assert len(frm) == 1

    # more complex schema
    schema = Schema(
        f"""
    timestamp int*
    category str*
    value int
    """
    )

    values = {
        "timestamp": [1589455901, 1589455901, 1589455902, 1589455902],
        "category": list("abab"),
        "value": [1, 2, 1, 2],
    }

    # drop first col
    frm = Frame(schema, values)
    partial_frm = frm.drop("timestamp")
    reduced_frm = partial_frm.reduce()
    assert len(reduced_frm) == 2
    assert list(reduced_frm["category"]) == ["a", "b"]
    assert list(reduced_frm["value"]) == [2, 4]

    # drop second
    partial_frm = frm.drop("category")
    reduced_frm = partial_frm.reduce()
    assert len(reduced_frm) == 2
    assert list(reduced_frm["timestamp"]) == [1589455901, 1589455902]
    assert list(reduced_frm["value"]) == [3, 3]

    # test with a 2-col partial index
    schema = Schema(
        f"""
    timestamp int*
    version int*
    category str*
    value int
    """
    )
    values = {
        "timestamp": [1589455901, 1589455901, 1589455902, 1589455902],
        "version": [1589455901, 1589455901, 1589455902, 1589455902],
        "category": list("abab"),
        "value": [1, 2, 1, 2],
    }

    # drop first col
    frm = Frame(schema, values)
    partial_frm = frm.drop("timestamp")
    reduced_frm = partial_frm.reduce()
    assert len(reduced_frm) == 4
    assert list(reduced_frm["category"]) == ["a", "b", "a", "b"]
    assert list(reduced_frm["value"]) == [1, 2, 1, 2]

    # drop third
    partial_frm = frm.drop("category")
    reduced_frm = partial_frm.reduce()
    assert len(reduced_frm) == 2
    assert list(reduced_frm["timestamp"]) == [1589455901, 1589455902]
    assert list(reduced_frm["value"]) == [3, 3]
