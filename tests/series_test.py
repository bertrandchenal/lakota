from datetime import timedelta

import pytest
from numpy import array
from pandas import DataFrame

from lakota import POD, Changelog, Frame, Repo, Schema, Series
from lakota.schema import DTYPES
from lakota.utils import tail, drange

schema = Schema(["timestamp int *", "value float"])
orig_frm = {
    "timestamp": [1589455903, 1589455904, 1589455905],
    "value": [3.3, 4.4, 5.5],
}


@pytest.fixture(
    scope="function",
)
def repo():
    return Repo("memory://")


@pytest.fixture(
    scope="function",
)
def series(request, repo):
    clct = repo.create_collection(schema, "-")
    series = clct / "_"
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

    assert series.frame() == orig_frm


def test_double_write(series):
    # Write some values
    commit = series.write(orig_frm)
    revs = list(series.revisions())
    assert commit is None
    assert series.frame() == orig_frm
    assert len(revs) == 1


@pytest.mark.parametrize("how", ["left", "right"])
def test_spill_write(series, how):
    if how == "left":
        ts = [1589455902, 1589455903, 1589455904, 1589455905]
        vals = [22, 33, 44, 55]
    else:
        ts = [1589455903, 1589455904, 1589455905, 1589455906]
        vals = [33, 44, 55, 66]

    frm = Frame(
        schema,
        {
            "timestamp": ts,
            "value": vals,
        },
    )
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

    frm = Frame(
        schema,
        {"timestamp": ts, "value": vals},
    )
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

    frm = Frame(
        schema,
        {
            "timestamp": ts,
            "value": vals,
        },
    )
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
    frm_copy = series[1589455902:1589455903].frame(closed="both")
    if how == "left":
        assert all(frm_copy["timestamp"] == [1589455902, 1589455903])
        assert all(frm_copy["value"] == [2.2, 3.3])

    else:
        assert all(frm_copy["timestamp"] == [1589455903])
        assert all(frm_copy["value"] == [3.3])

    # Slice read - right slice
    frm_copy = series[1589455905:1589455906].frame(closed="both")
    if how == "left":
        assert all(frm_copy["timestamp"] == [1589455905])
        assert all(frm_copy["value"] == [5.5])

    else:
        assert all(frm_copy["timestamp"] == [1589455905, 1589455906])
        assert all(frm_copy["value"] == [5.5, 6.6])


def test_column_types(repo):
    names = [dt.name for dt in DTYPES]
    cols = [f"{n} {n}" for n in names]
    df = {n: array([0], dtype=n) for n in names}

    for idx_len in range(1, len(cols)):
        stars = ["*"] * idx_len + [""] * (len(cols) - idx_len)
        schema = Schema([c + star for c, star in zip(cols, stars)])
        clct = repo.create_collection(schema, "-")
        series = clct / "_"
        series.write(df)
        frm = series.frame()

        for name in names:
            assert all(frm[name] == df[name])


def test_kv_series(repo):
    schema = Schema(["timestamp timestamp*", "category str*", "value int"], kind="kv")
    clct = repo.create_collection(schema, "-")
    series = clct / "_"

    frm = {
        "timestamp": ["2020-01-01", "2020-02-01", "2020-03-01"],
        "category": ["a", "c", "d"],
        "value": [1, 2, 3],
    }
    series.write(frm)
    frm = {
        "timestamp": ["2020-01-01", "2020-02-02", "2020-02-03"],
        "category": ["a", "b", "c"],
        "value": [4, 5, 6],
    }
    series.write(frm)
    res = series.frame()["value"]
    assert all(res == [4, 2, 5, 6, 3])

    # Double-write should be a noop
    commit = series.write(frm)
    assert commit is None


def test_rev_filter(series):
    # Read those back
    second_frm = {
        "timestamp": [1589455904, 1589455905],
        "value": [44, 55],
    }
    series.write(second_frm)
    (last_rev,) = tail(series.revisions())

    # Read initial commit
    old_frm = series.frame(before=last_rev["epoch"])
    assert old_frm == orig_frm

    # Ignore initial commit
    new_frm = series.frame(after=last_rev["epoch"])
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
        {
            "timestamp": [1589455906, 1589455907, 1589455908],
            "value": [6, 7, 8],
        }
    )
    series.write(
        {
            "timestamp": [1589455909, 1589455910, 1589455911],
            "value": [9, 10, 11],
        }
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
    frames = series.paginate(2, offset=1)
    res = []
    for frm in frames:
        res.extend(frm["timestamp"])
    assert res == list(range(1589455904, 1589455912))

    # Same with offset and limit
    frames = series.paginate(2, offset=1, limit=5)
    res = []
    for frm in frames:
        res.extend(frm["timestamp"])
    assert res == list(range(1589455904, 1589455909))

    # Same with offset and limit
    frames = series.paginate(10, offset=1, limit=5)
    res = []
    for frm in frames:
        res.extend(frm["timestamp"])
    assert res == list(range(1589455904, 1589455909))

    # Same with offset and limit
    frames = series.paginate(offset=10, limit=5)
    res = []
    for frm in frames:
        res.extend(frm["timestamp"])
    assert res == []


def test_partition(repo):
    schema = Schema(["timestamp timestamp*", "value float"])
    clct = repo.create_collection(schema, "timeseries")
    series = clct / "_"

    # Start with a low-density frame and add higher density frames
    deltas = [
        timedelta(days=1),
        timedelta(hours=1),
        timedelta(minutes=1),
        timedelta(seconds=1),
        timedelta(milliseconds=100),
    ]
    partitions = [None, None, 'Y', 'W', 'D']
    for delta, partition in zip(deltas, partitions):
        ts = drange('2020-01-01', '2020-01-10', delta)
        frm = {
            "timestamp": ts,
            "value": range(len(ts)),
        }
        series.write(frm)
        itv = series.interval()
        assert itv == partition
