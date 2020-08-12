import pytest
from numpy import append

from baltic import POD, Frame, Registry, Schema
from baltic.utils import hashed_path

NAMES = list("abcde")
VALUES = [1.1, 2.2, 3.3, 4.4, 5.5]


@pytest.fixture
def frame():
    return {
        "value": VALUES,
        "category": NAMES,
    }


@pytest.fixture
def frm(frame):
    schema = Schema(["category:str", "value:float"])

    frm = Frame(schema)
    frm.write(frame)
    return frm


def test_copy_frame(frm):
    pod = POD.from_uri("memory://")
    digests = frm.save(pod)
    for col, dig in zip(frm.schema.columns, digests):
        folder, filename = hashed_path(dig)
        data = pod.read(folder / filename)
        arr = frm.schema[col].decode(data)
        assert (frm[col] == arr).all()


def test_copy(frm):
    frm2 = Frame.concat(frm.schema, frm)
    assert frm == frm2


def test_concat(frm):
    frm2 = Frame.concat(frm.schema, frm, frm)
    val2 = frm2["value"]
    val = frm["value"]
    eq = val2 == append(val, val)
    assert all(eq)


def test_index_slice():

    schema = Schema(["x:int"])
    frm = Frame(schema)
    frm.write({"x": [1, 2, 3, 4, 5, 5, 5, 6]})

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


def test_double_slice(frame, frm):
    # in-meory frame
    frm = frm.slice(slice(1, None)).slice(slice(None, 2))
    assert all(frm["value"] == VALUES[1:][:2])

    # frame created from registry
    reg = Registry()
    (series,) = reg.create(frm.schema, "my-label")
    series.write(frame)
    frm = series.read()
    frm = frm.slice(slice(1, None)).slice(slice(None, 2))
    assert all(frm["value"] == VALUES[1:][:2])

    # Add chunks
