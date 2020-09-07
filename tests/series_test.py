import pytest
from numpy import array
from pandas import DataFrame

from lakota import POD, Frame, Schema, Series
from lakota.schema import DTYPES
from lakota.utils import tail

schema = Schema(["timestamp int *", "value float"])
orig_frm = {
    "timestamp": [1589455903, 1589455904, 1589455905],
    "value": [3.3, 4.4, 5.5],
}


@pytest.fixture(scope="function",)
def series(request):
    pod = POD.from_uri("memory://")
    series = Series("_", schema, pod)
    series.write(orig_frm)
    return series


def test_read_series(series):
    # Read those back
    frm_copy = series.frame()
    assert frm_copy == orig_frm


def test_write_df(series):
    # Write some values
    df = DataFrame(orig_frm)
    series.write(df)  # TODO implement setitem

    assert series[:] == orig_frm


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

    frm_copy = series.frame()
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

    frm_copy = series.frame()
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
    frm_copy = series.frame()
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
    frm_copy = series.closed("both")[1589455902:1589455903]
    if how == "left":
        assert all(frm_copy["timestamp"] == [1589455902, 1589455903])
        assert all(frm_copy["value"] == [2.2, 3.3])

    else:
        assert all(frm_copy["timestamp"] == [1589455903])
        assert all(frm_copy["value"] == [3.3])

    # Slice read - right slice
    frm_copy = series.closed("both")[1589455905:1589455906]
    if how == "left":
        assert all(frm_copy["timestamp"] == [1589455905])
        assert all(frm_copy["value"] == [5.5])

    else:
        assert all(frm_copy["timestamp"] == [1589455905, 1589455906])
        assert all(frm_copy["value"] == [5.5, 6.6])


def test_column_types():
    names = [dt.name for dt in DTYPES]
    cols = [f"{n} {n}" for n in names]
    df = {n: array([0], dtype=n) for n in names}

    for idx_len in range(1, len(cols)):
        pod = POD.from_uri("memory://")
        stars = ["*"] * idx_len + [""] * (len(cols) - idx_len)
        schema = Schema([c + star for c, star in zip(cols, stars)])
        series = Series("_", schema, pod)
        series.write(df)
        frm = series.frame()

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
    old_frm = series.before(last_rev["epoch"]).frame()
    assert old_frm == orig_frm

    # Ignore initial commit
    new_frm = series.after(last_rev["epoch"]).frame()
    assert new_frm == second_frm


@pytest.mark.parametrize("extra_commit", [True, False])
def test_paginate(series, extra_commit):
    ts = orig_frm["timestamp"]
    frm = next(series.paginate(step=3))
    assert all(frm["timestamp"] == ts)

    frames = series.paginate(step=1)
    for frm, val in zip(frames, ts):
        assert len(frm) == 1
        assert frm["timestamp"][0] == val

    frames = list(series.paginate(step=2))
    assert all(frames[0]["timestamp"] == ts[:2])
    assert all(frames[1]["timestamp"] == ts[2:])

    # Add two commits
    series.write(
        {"timestamp": [1589455906, 1589455907, 1589455908], "value": [6, 7, 8],}
    )
    series.write(
        {"timestamp": [1589455909, 1589455910, 1589455911], "value": [9, 10, 11],}
    )
    if extra_commit:
        series.write(
            {
                "timestamp": [1589455907, 1589455908, 1589455909, 1589455910],
                "value": [7, 8, 9, 10],
            }
        )

    # Paginate and reassemble
    frames = series.paginate(2)
    res = []
    for frm in frames:
        res.extend(frm["timestamp"])
    assert res == list(range(1589455903, 1589455912))

    # Same with offset
    frames = series.offset(1).paginate(2)
    res = []
    for frm in frames:
        res.extend(frm["timestamp"])
    assert res == list(range(1589455904, 1589455912))

    # Same with offset and limit
    frames = series.offset(1).limit(5).paginate(2)
    res = []
    for frm in frames:
        res.extend(frm["timestamp"])
    assert res == list(range(1589455904, 1589455909))

    # Same with offset and limit
    frames = series.offset(1).limit(5).paginate(10)
    res = []
    for frm in frames:
        res.extend(frm["timestamp"])
    assert res == list(range(1589455904, 1589455909))

    # Same with offset and limit
    frames = series.offset(10).limit(5).paginate()
    res = []
    for frm in frames:
        res.extend(frm["timestamp"])
    assert res == []
