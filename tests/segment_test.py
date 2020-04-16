from uuid import uuid4
from numpy import array

import zarr

from baltic import Segment, Schema

def test_write_segment():
    FACTOR = 100_000
    names = [str(uuid4()) for _ in range(5)]
    frame = {
        'value': array([1.1, 2.2, 3.3, 4.4, 5.5] * FACTOR),
        'category': array(names * FACTOR),
    }
    schema = Schema(['category'], ['value'])
    segment = Segment(schema)
    segment.save(frame)
    res = segment.read('category', 'value')

    for col in res:
        assert (res[col][:] == frame[col]).all()
