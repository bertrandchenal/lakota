from uuid import uuid4

import pytest
from numpy import append, array

from baltic import POD, Frame, Schema
from baltic.utils import hashed_path


@pytest.fixture
def frame():
    FACTOR = 100_000
    names = [str(uuid4()) for _ in range(5)]
    return {
        "value": array([1.1, 2.2, 3.3, 4.4, 5.5] * FACTOR),
        "category": array(names * FACTOR),
    }


@pytest.fixture
def sgm(frame):
    schema = Schema(["category:str", "value:float"])

    sgm = Frame(schema)
    sgm.write(frame)
    return sgm


def test_copy_frame(sgm):
    pod = POD.from_uri("memory://")
    digests = sgm.save(pod)
    for col, dig in zip(sgm.schema.columns, digests):
        folder, filename = hashed_path(dig)
        data = pod.read(folder / filename)
        arr = sgm.schema[col].decode(data)
        assert (sgm[col] == arr).all()


def test_copy(sgm):
    sgm2 = Frame.concat(sgm.schema, sgm)
    assert sgm == sgm2


def test_concat(sgm):
    sgm2 = Frame.concat(sgm.schema, sgm, sgm)
    val2 = sgm2["value"]
    val = sgm["value"]
    eq = val2 == append(val, val)
    assert all(eq)


def test_index_slice(sgm):

    schema = Schema(["x:int"])
    sgm = Frame(schema)
    sgm.write({"x": [1, 2, 3, 4, 5, 5, 5, 6]})

    # include both side
    res = sgm.index_slice([2], [4], closed="both")["x"]
    assert all(res == [2, 3, 4])

    # include only left
    res = sgm.index_slice([2], [4], closed="left")["x"]
    assert all(res == [2, 3])

    # include only right
    res = sgm.index_slice([2], [4], closed="right")["x"]
    assert all(res == [3, 4])

    # implict right
    res = sgm.index_slice([5])["x"]
    assert all(res == [5, 5, 5])

    res = sgm.index_slice([1])["x"]
    assert all(res == [1])

    res = sgm.index_slice([6])["x"]
    assert all(res == [6])
