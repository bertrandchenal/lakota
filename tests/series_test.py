from numpy import array
import pytest

from baltic import POD, Schema, Segment, Series
from baltic.schema import DTYPES

schema = Schema(["timestamp:int", "value:float"])
sgm = Segment.from_df(
    schema,
    {"timestamp": [1589455903, 1589455904, 1589455905], "value": [3.3, 4.4, 5.5],},
)

@pytest.fixture
def series():
    pod = POD.from_uri("memory://")
    series = Series(schema, pod)

    # Write some values
    series.write(sgm)
    return series

def test_read_series(series):
    # Read those back
    sgm_copy = series.read()
    assert sgm_copy == sgm


def test_overlaping_write(series):
    # Double write
    series.write(sgm)
    sgm_copy = series.read()
    assert sgm_copy == sgm

@pytest.mark.parametrize('how', ['left', 'right'])
def test_spill_write(series, how):
    if how == 'left':
        ts = [1589455902, 1589455903, 1589455904, 1589455905]
        vals = [22, 33, 44, 55]
    else:
        ts = [1589455903, 1589455904, 1589455905, 1589455906]
        vals = [33, 44, 55, 66]

    sgm = Segment.from_df(
        schema,
        {"timestamp": ts, "value": vals,},
    )
    series.write(sgm)

    sgm_copy = series.read()
    assert sgm_copy == sgm


@pytest.mark.parametrize('how', ['left', 'right'])
def test_short_cover(series, how):
    if how == 'left':
        ts = [1589455904, 1589455905]
        vals = [44, 55]
    else:
        ts = [1589455903, 1589455904]
        vals = [33, 44]

    sgm = Segment.from_df(
        schema,
        {"timestamp": ts, "value": vals,},
    )
    series.write(sgm)

    sgm_copy = series.read()
    assert all(sgm_copy['timestamp'] == [1589455903, 1589455904, 1589455905])
    if how == 'left':
        assert all(sgm_copy['value'] == [3.3, 44, 55])

    else:
        assert all(sgm_copy['value'] == [33, 44, 5.5])

@pytest.mark.parametrize('how', ['left', 'right'])
def test_adjacent_write(series, how):
    if how == 'left':
        ts = [1589455902]
        vals = [2.2]
    else:
        ts = [1589455906]
        vals = [6.6]

    sgm = Segment.from_df(
        schema,
        {"timestamp": ts, "value": vals,},
    )
    series.write(sgm)

    # Full read
    sgm_copy = series.read()
    if how == 'left':
        assert all(sgm_copy['timestamp'] == [1589455902, 1589455903, 1589455904, 1589455905])
        assert all(sgm_copy['value'] == [2.2, 3.3, 4.4, 5.5])

    else:
        assert all(sgm_copy['timestamp'] == [1589455903, 1589455904, 1589455905, 1589455906])
        assert all(sgm_copy['value'] == [3.3, 4.4, 5.5, 6.6])

    # Slice read - left slice
    sgm_copy = series.read(start=[1589455902], end=[1589455903])
    if how == 'left':
        assert all(sgm_copy['timestamp'] == [1589455902, 1589455903])
        assert all(sgm_copy['value'] == [2.2, 3.3])

    else:
        assert all(sgm_copy['timestamp'] == [1589455903])
        assert all(sgm_copy['value'] == [3.3])

    # Slice read - right slice
    sgm_copy = series.read(start=[1589455905], end=[1589455906])
    if how == 'left':
        assert all(sgm_copy['timestamp'] == [1589455905])
        assert all(sgm_copy['value'] == [5.5])

    else:
        assert all(sgm_copy['timestamp'] == [1589455905, 1589455906])
        assert all(sgm_copy['value'] == [5.5, 6.6])



def test_column_types():
    names = [dt.name for dt in DTYPES]
    cols = [f"{n}:{n}" for n in names]
    df = {n: array([0], dtype=n) for n in names}

    for idx_len in range(1, len(cols)):
        pod = POD.from_uri("memory://")
        schema = Schema(cols, idx_len)
        series = Series(schema, pod)
        series.write(df)
        sgm = series.read()

        for name in names:
            assert all(sgm[name] == df[name])
