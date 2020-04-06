from hashlib import sha1
from numpy import array
from os import listdir
from os.path import join
from tempfile import TemporaryDirectory
from uuid import uuid4


import zarr

from baltic import Segment, RefLog


def test_write_segment():
    FACTOR = 100_000
    names = [str(uuid4()) for _ in range(5)]
    frame = {
        'value': array([1.1, 2.2, 3.3, 4.4, 5.5] * FACTOR),
        'category': array(names * FACTOR),
    }
    schema = {
        'dimensions': ['category'],
        'measures': ['value'],
    }
    gr = zarr.TempStore()
    segment = Segment(gr, schema)
    segment.save(frame)
    res = segment.read('category', 'value')

    for col in res:
        assert (res[col][:] == frame[col]).all()


def test_create_refs():
    # Create 3 changeset in series
    datum = b'ham spam foo bar baz'.split()
    with TemporaryDirectory() as td:
        reflog = RefLog(td)
        for data in datum:
            name = sha1(data).hexdigest()
            reflog.save(name, data)

        res = listdir(td)
        assert len(res) > 0

        for name, expected in zip(reflog.walk(), datum):
            data = open(join(td, name), 'rb').read()
            assert data == expected
