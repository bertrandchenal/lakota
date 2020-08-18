import pytest
from numpy import array
from pandas import DataFrame

from baltic import POD, Frame, Schema, Series
from baltic.schema import DTYPES
from baltic.utils import tail

schema = Schema(["timestamp:int", "value:float"])
frm = {
    "timestamp": [1589455903, 1589455904, 1589455905],
    "value": [3.3, 4.4, 5.5],
}


@pytest.fixture(scope="function", params=["dataframe", "dict"])
def series(request):
    pod = POD.from_uri("memory://")
    series = Series("_", schema, pod)
    # Write some values
    if request.param == "dataframe":
        df = DataFrame(frm)
        series.write(df)
    else:
        series.write(frm)
    return series


def test_read_series(series):
    # Read those back
    frm_copy = series.read()
    assert frm_copy == frm


@pytest.mark.parametrize("how", ["left", "right"])
def test_spill_write(series, how):
    if how == "left":
        ts = [1589455902, 1589455903, 1589455904, 1589455905]
        vals = [22, 33, 44, 55]
    else:
        ts = [1589455903, 1589455904, 1589455905, 1589455906]
        vals = [33, 44, 55, 66]

    frm = Frame(schema, {"timestamp": ts, "value": vals,})
    series.write(frm)

    frm_copy = series.read()
    assert frm_copy == frm


@pytest.mark.parametrize("how", ["left", "right"])
def test_short_cover(series, how):
    if how == "left":
        ts = [1589455904, 1589455905]
        vals = [44, 55]
    else:
        ts = [1589455903, 1589455904]
        vals = [33, 44]

    frm = Frame(schema, {"timestamp": ts, "value": vals},)
    series.write(frm)

    frm_copy = series.read()
    assert all(frm_copy["timestamp"] == [1589455903, 1589455904, 1589455905])
    if how == "left":
        assert all(frm_copy["value"] == [3.3, 44, 55])

    else:
        assert all(frm_copy["value"] == [33, 44, 5.5])


@pytest.mark.parametrize("how", ["left", "right"])
def test_adjacent_write(series, how):
    if how == "left":
        ts = [1589455902]
        vals = [2.2]
    else:
        ts = [1589455906]
        vals = [6.6]

    frm = Frame(schema, {"timestamp": ts, "value": vals,},)
    series.write(frm)

    # Full read
    frm_copy = series.read()
    if how == "left":
        assert all(
            frm_copy["timestamp"] == [1589455902, 1589455903, 1589455904, 1589455905]
        )
        assert all(frm_copy["value"] == [2.2, 3.3, 4.4, 5.5])

    else:
        assert all(
            frm_copy["timestamp"] == [1589455903, 1589455904, 1589455905, 1589455906]
        )
        assert all(frm_copy["value"] == [3.3, 4.4, 5.5, 6.6])

    # Slice read - left slice
    frm_copy = series.read(start=[1589455902], end=[1589455903])
    if how == "left":
        assert all(frm_copy["timestamp"] == [1589455902, 1589455903])
        assert all(frm_copy["value"] == [2.2, 3.3])

    else:
        assert all(frm_copy["timestamp"] == [1589455903])
        assert all(frm_copy["value"] == [3.3])

    # Slice read - right slice
    frm_copy = series.read(start=[1589455905], end=[1589455906])
    if how == "left":
        assert all(frm_copy["timestamp"] == [1589455905])
        assert all(frm_copy["value"] == [5.5])

    else:
        assert all(frm_copy["timestamp"] == [1589455905, 1589455906])
        assert all(frm_copy["value"] == [5.5, 6.6])


def test_column_types():
    names = [dt.name for dt in DTYPES]
    cols = [f"{n}:{n}" for n in names]
    df = {n: array([0], dtype=n) for n in names}

    for idx_len in range(1, len(cols)):
        pod = POD.from_uri("memory://")
        schema = Schema(cols, idx_len)
        series = Series("_", schema, pod)
        series.write(df)
        frm = series.read()

        for name in names:
            assert all(frm[name] == df[name])


def test_rev_filter(series):
    # Read those back
    second_frm = {
        "timestamp": [1589455904, 1589455905],
        "value": [44, 55],
    }
    series.write(second_frm)
    (last_rev,) = tail(series.revisions())

    # Read initial commit
    old_frm = series.read(before=last_rev["epoch"])
    assert old_frm == frm

    # Ignore initial commit
    new_frm = series.read(after=last_rev["epoch"])
    assert new_frm == second_frm
