from uuid import uuid4

import pytest
from numpy import append, array

from baltic import POD, Schema, Segment


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

    sgm = Segment(schema)
    sgm.write(frame)
    return sgm


def test_copy_segment(sgm):
    pod = POD.from_uri("memory://")
    digests = sgm.save(pod)
    for col, dig in zip(sgm.schema.columns, digests):
        prefix, suffix = dig[:2], dig[2:]
        data = pod.read(f"{prefix}/{suffix}")
        arr = sgm.schema.decode(col, data)
        assert (sgm[col] == arr).all()


def test_concat(sgm):
    sgm2 = Segment.concat(sgm.schema, sgm, sgm)
    val2 = sgm2["value"]
    val = sgm["value"]
    eq = val2 == append(val, val)
    assert all(eq)
