from numpy import array

from baltic import POD, Schema, Segment, Series
from baltic.schema import DTYPES

def test_write_series():
    pod = POD.from_uri("memory://")
    schema = Schema(["timestamp:int", "value:float"])
    series = Series(schema, pod)

    # Write some values
    sgm = Segment.from_df(
        schema,
        {"timestamp": [1589455903, 1589455904, 1589455905], "value": [1.1, 2.2, 3.3],},
    )
    series.write(sgm)

    # Read those back
    sgm_copy = series.read()
    assert sgm_copy == sgm

def test_column_types():
    names = [dt.name for dt in DTYPES]
    cols = [f'{n}:{n}' for n in names]
    df = {n: array([0], dtype=n) for n in names}

    for idx_len in range(1, len(cols)):
        pod = POD.from_uri("memory://")
        schema = Schema(cols, idx_len)
        series = Series(schema, pod)
        series.write(df)
        sgm = series.read()

        for name in names:
            assert all(sgm[name] == df[name])


# TODO implement push & pull
