import pytest

from baltic import Frame, Registry, Schema

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
    schema = Schema(["category:str", "value:float"])

    frm = Frame(schema, frame_values)
    return frm


def test_index_slice():
    schema = Schema(["x:int"])
    frm = Frame(schema, {"x": [1, 2, 3, 4, 5, 5, 5, 6]})

    # include both side
    res = frm.index_slice([2], [4], closed="both")["x"]
    assert res == [2, 3, 4]

    # include only left
    res = frm.index_slice([2], [4], closed="left")["x"]
    assert res == [2, 3]

    # include only right
    res = frm.index_slice([2], [4], closed="right")["x"]
    assert res == [3, 4]

    # implict right
    res = frm.index_slice([5], [5], closed="both")["x"]
    assert res == [5, 5, 5]

    res = frm.index_slice([1], [1], closed="both")["x"]
    assert res == [1]

    res = frm.index_slice([6], [6], closed="both")["x"]
    assert res == [6]


def test_double_slice(frame_values, frm):
    # in-meory frame
    frm = frm.slice(1, None).slice(None, 2)
    assert frm["value"] == VALUES[1:][:2]

    # frame created from registry
    reg = Registry()
    (series,) = reg.create(frm.schema, "my-label")
    series.write(frame_values)
    frm = series.read()
    frm = frm.slice(1, None).slice(None, 2)
    assert all(frm["value"] == VALUES[1:][:2])

    # Add chunks
