import zarr

from baltic import Series, Schema, Segment

def test_write_series():
    schema = Schema(['timestamp:int', 'value:float'])
    series = Series(schema, zarr.group())

    # Write some values
    sgm = Segment.from_df(schema, {
        'timestamp': [1589455903, 1589455904, 1589455905],
        'value': [1.1, 2.2, 3.3],
    })
    series.write(sgm)

    # Read those back
    sgm_copy = series.read()
    assert sgm_copy == sgm
