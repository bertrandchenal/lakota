from datetime import datetime
from random import shuffle
from time import sleep

import pytest
from numpy import asarray
from pandas import DataFrame

from lakota import Frame, Repo, Schema
from lakota.schema import ALIASES

# Default schema with some data
schema = Schema(timestamp="int *", value="float")
orig_frm = {
    "timestamp": [1589455903, 1589455904, 1589455905],
    "value": [3.3, 4.4, 5.5],
}


## TODO write/adapt tests for multi series
# Same with multi-index
multi_schema = Schema(timestamp="int *", version="int *",  value="float")
multi_orig_frm = {
    "timestamp": [1589455903, 1589455903, 1589455904, 1589455904, 1589455905, 1589455905],
    "version": [1, 2, 1, 2, 1, 2],
    "value": [3.3, 4.4, 5.5, 6.6, 7.7, 8.8],
}


@pytest.fixture(
    scope="function",
)
def repo():
    return Repo("memory://")


@pytest.fixture(
    scope="function",
)
def multi_series(request, repo):
    clct = repo.create_collection(multi_schema, "--")
    series = clct / "_"
    series.write(multi_orig_frm)
    return series

@pytest.fixture(
    scope="function",
)
def series(request, repo):
    clct = repo.create_collection(schema, "-")
    series = clct / "_"
    series.write(orig_frm)
    return series

@pytest.fixture(
    scope="function",
)
def empty_series(request, repo):
    clct = repo.create_collection(schema, "-", raise_if_exists=False)
    series = clct / "empty"
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
    revs = list(series.changelog.log())
    assert not commit
    assert series.frame() == orig_frm
    assert len(revs) == 1


@pytest.mark.parametrize("how", ["left", "right"])
def test_spill_write(series, how):
    '''
    test a write that overlap the current data but with one extra row
    before or after.
    '''
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

    # Test full read
    args = [
        # closed is both (default)
        (None, None, "b"),
        (min(ts), max(ts), "b"),
        (None, max(ts), "b"),
        (min(ts), None, "b"),
        # open on left
        (min(ts) - 1, max(ts), "r"),
        # open on right
        (min(ts), max(ts) + 1, "l"),
        # full open
        (min(ts) - 1, max(ts) + 1, "n"),
    ]
    for start, stop, closed in args:
        frm_copy = series.frame(start=start, stop=stop, closed=closed)
        assert frm_copy == frm

    # Test partial read
    expected = Frame(
        schema,
        {
            "timestamp": [1589455903, 1589455904],
            "value": [33, 44],
        },
    )
    args = [
        # closed is both (default)
        (1589455903, 1589455904, "b"),
        # Open on left
        (1589455902, 1589455904, "r"),
        # # open on right
        (1589455903, 1589455905, "l"),
        # # open on both
        (1589455902, 1589455905, "n"),
    ]
    for start, stop, closed in args:
        frm_copy = series.frame(start=start, stop=stop, closed=closed)
        assert frm_copy == expected


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
        ts = [1589455901, 1589455902]
        vals = [1.1, 2.2]
    else:
        ts = [1589455906, 1589455907]
        vals = [6.6, 7.7]

    # We do two write of one arrays (should trigger more corner cases)
    for pos, stamp in enumerate(ts):
        frm = Frame(
            schema,
            {
                "timestamp": [stamp],
                "value": [vals[pos]],
            },
        )
        series.write(frm)

    # Full read
    frm_copy = series.frame()
    if how == "left":
        assert all(
            frm_copy["timestamp"]
            == [1589455901, 1589455902, 1589455903, 1589455904, 1589455905]
        )
        assert all(frm_copy["value"] == [1.1, 2.2, 3.3, 4.4, 5.5])

    else:
        assert all(
            frm_copy["timestamp"]
            == [1589455903, 1589455904, 1589455905, 1589455906, 1589455907]
        )
        assert all(frm_copy["value"] == [3.3, 4.4, 5.5, 6.6, 7.7])

    # Slice read - left slice
    frm_copy = series.frame(1589455902, 1589455903, closed="b")
    if how == "left":
        assert all(frm_copy["timestamp"] == [1589455902, 1589455903])
        assert all(frm_copy["value"] == [2.2, 3.3])

    else:
        assert all(frm_copy["timestamp"] == [1589455903])
        assert all(frm_copy["value"] == [3.3])

    # Slice read - right slice
    frm_copy = series.frame(1589455905, 1589455906, closed="b")
    if how == "left":
        assert all(frm_copy["timestamp"] == [1589455905])
        assert all(frm_copy["value"] == [5.5])

    else:
        assert all(frm_copy["timestamp"] == [1589455905, 1589455906])
        assert all(frm_copy["value"] == [5.5, 6.6])

@pytest.mark.parametrize("cols", [['timestamp'], ['timestamp', 'value'], ['value']])
def test_select(series, empty_series, cols):
    df = series.df(select=cols)
    assert list(df) == cols
    assert len(df) > 1

    df = empty_series.df(select=cols)
    assert list(df) == cols
    assert len(df) ==0

    df = series.df(start=1589455905+1, select=cols)
    assert list(df) == cols
    assert len(df) == 0


def test_write_open_left(series):
    # Append
    frm = {
        "timestamp": [1589455906, 1589455907],
        "value": [6.6, 7.7],
    }
    series.write(
        frm,
        start=1589455905,  # last value of series
        closed="r",
    )
    expected = [1589455903, 1589455904, 1589455905, 1589455906, 1589455907]
    assert all(series.frame(select="timestamp")["timestamp"] == expected)

    # Append again, hide part of previous write
    frm = {
        "timestamp": [1589455907],
        "value": [7],
    }
    series.write(
        frm,
        start=1589455905,  # last value of initial series
        closed="r",
    )
    res = series.frame()
    assert all(
        res["timestamp"]
        == [1589455903, 1589455904, 1589455905, 1589455907]  # 1589455906 is missing
    )

    assert all(res["value"] == [3.3, 4.4, 5.5, 7])

    # partial read
    res = series.frame(start=1589455905, closed="r")
    assert all(res["timestamp"] == [1589455907])
    res = series.frame(start=1589455906, closed="b")
    assert all(res["timestamp"] == [1589455907])


def test_write_open_right(series):
    # preprend
    frm = {
        "timestamp": [1589455901, 1589455902],
        "value": [1.1, 2.2],
    }
    series.write(
        frm,
        stop=1589455903,  # first value of series
        closed="l",
    )
    expected = [1589455901, 1589455902, 1589455903, 1589455904, 1589455905]
    assert all(series.frame(select="timestamp")["timestamp"] == expected)

    # Append again, hide part of previous write
    frm = {
        "timestamp": [1589455901],
        "value": [1],
    }
    series.write(
        frm,
        stop=1589455903,  # last value of initial series
        closed="l",
    )
    res = series.frame()
    assert all(
        res["timestamp"]
        == [1589455901, 1589455903, 1589455904, 1589455905]  # 1589455902 is missing
    )

    assert all(res["value"] == [1, 3.3, 4.4, 5.5])

    # partial read
    res = series.frame(stop=1589455901, closed="l")
    assert all(res["timestamp"] == [1589455901])
    res = series.frame(stop=1589455902, closed="b")
    assert all(res["timestamp"] == [1589455901])


def test_write_open_center(series):
    # insert
    frm = {
        "timestamp": [1589455904],
        "value": [4],
    }
    series.write(
        frm,
        start=1589455903,
        stop=1589455905,
        closed="n",
    )
    frm = series.frame()
    assert all(frm["timestamp"] == [1589455903, 1589455904, 1589455905])
    assert all(frm["value"] == [3.3, 4, 5.5])

    # center left
    frm = {
        "timestamp": [1589455903],
        "value": [3],
    }
    series.write(
        frm,
        start=1589455902,
        closed="r",
    )
    frm = series.frame()
    assert all(frm["timestamp"] == [1589455903, 1589455904, 1589455905])
    assert all(frm["value"] == [3, 4, 5.5])

    # center right
    frm = {
        "timestamp": [1589455905],
        "value": [5],
    }
    series.write(
        frm,
        stop=1589455906,
        closed="l",
    )
    frm = series.frame()
    assert all(frm["timestamp"] == [1589455903, 1589455904, 1589455905])
    assert all(frm["value"] == [3, 4, 5])


def test_column_types(repo):
    df = {str(dt): asarray([0], dtype=ALIASES[dt]) for dt in ALIASES}

    for idx_len in range(1, len(ALIASES)):
        stars = ["*"] * idx_len + [""] * (len(ALIASES) - idx_len)
        schema = Schema(**{c: c + star for c, star in zip(ALIASES, stars)})
        clct = repo.create_collection(schema, str(idx_len))
        series = clct / "-"
        series.write(df)
        frm = series.frame()
        for dt in ALIASES:
            assert all(frm[str(dt)] == df[str(dt)])


def test_kv_series(repo):
    schema = Schema.kv(timestamp="timestamp*", category="str*", value="int")
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
    assert not commit


def test_rev_filter(series):
    # Read those back
    second_frm = {
        "timestamp": [1589455904, 1589455905],
        "value": [44, 55],
    }
    sleep(0.1)  # Make sure we don't have a race condition
    series.write(second_frm)
    last_rev = series.changelog.leaf()

    # Read initial commit
    old_frm = series.frame(before=last_rev.epoch)
    assert old_frm == orig_frm

    # Pass an actual timestamp instead of hextime
    ts = int(last_rev.epoch, base=16)
    ts = datetime.fromtimestamp(ts / 1000)
    old_frm = series.frame(before=ts)
    assert old_frm == orig_frm


@pytest.mark.parametrize("extra_commit", [True, False])
def test_paginate(repo, extra_commit):
    clct = repo.create_collection(schema, "paginate")
    series = clct / "base"
    series.write(orig_frm)

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


    # Test series with zero line
    series = clct / "zero"
    frames = list(series.paginate())
    assert len(frames) == 0

    # Test series with one line
    series = clct / "one"
    series.write(
        {
            "timestamp": [1589455909],
            "value": [9],
        }
    )
    frames = list(series.paginate(step=1))
    assert len(frames) == 1
    assert len(frames[0]) == 1

    # Test series with overlap on bound (generate a closed-left
    # segment)
    series = clct / "two"
    series.write(
        {
            "timestamp": [1589455908, 1589455909],
            "value": [8, 9],
        }
    )
    series.write(
        {
            "timestamp": [1589455908],
            "value": [8],
        }
    )
    frames = list(series.paginate(step=1))
    assert len(frames) == 2
    assert len(frames[0]) == 1


@pytest.mark.parametrize("direction", ["fwd", "bwd", "rand"])
@pytest.mark.parametrize("sgm_size", [1, 2, 3])
def test_fragmented_write(series, direction, sgm_size):
    ts = [1589455901, 1589455902, 1589455903, 1589455904, 1589455905, 1589455906]
    vals = [11, 22, 33, 44, 55, 66]

    if direction == "fwd":
        rg = range(len(ts))
    elif direction == "bwd":
        rg = range(len(ts) - 1, -1, -1)
    else:
        rg = list(range(len(ts)))
        shuffle(rg)
    for pos in rg:
        frm = Frame(
            schema,
            {
                "timestamp": ts[pos : pos + sgm_size],
                "value": vals[pos : pos + sgm_size],
            },
        )
        series.write(frm)

    frm = series.frame()
    assert all(frm["timestamp"] == ts)
    assert all(frm["value"] == vals)


@pytest.mark.parametrize("multi", (True, False))
def test_write_oneliner(repo, multi):
    clct = repo.create_collection(schema, "-")
    data = {'timestamp': [0], 'value': [0]}
    labels = ('ham', 'spam', 'foo')
    if multi:
        with clct.multi():
            for lb in labels:
                clct.series(lb).write(data)
    else:
        for lb in labels:
            clct.series(lb).write(data)
    assert clct.ls() == sorted(labels)

@pytest.mark.parametrize("col_type", list(ALIASES))
def test_update(repo, col_type):
    dtype = ALIASES[col_type]
    schema = Schema(timestamp="timestamp*", a=col_type, b=col_type)
    clct = repo.create_collection(schema, "-")
    series = clct / "_"
    frm = {
        "timestamp": ["2020-01-01", "2020-02-01", "2020-03-01"],
        "a": [1, 2, 3],
        "b": [1, 2, 3],
    }
    series.write(frm)

    # write over full index
    frm = {
        "timestamp": ["2020-01-01", "2020-02-01", "2020-03-01"],
        "a": [10, 20, 30],
    }
    series.update(frm)
    frm = series.frame()
    expected = asarray([10, 20, 30], dtype=dtype)
    assert (frm["a"] == expected).all()

    # With extra item at the end
    frm = {
        "timestamp": ["2020-02-01", "2020-03-01", "2020-04-01"],
        "a": [200, 300, 400],
    }
    series.update(frm)
    frm = series.frame()
    expected_a = asarray([10, 200, 300, 400], dtype=dtype)
    if col_type == "str":
        expected_b = ["1", "2", "3", ""]
    else:
        expected_b = asarray([1, 2, 3, 0], dtype=dtype)
    assert (frm["a"] == expected_a).all()
    assert (frm["b"] == expected_b).all()

    # With extra item at the start
    frm = {
        "timestamp": ["2019-12-01", "2020-01-01", "2020-02-01", "2020-03-01"],
        "a": [0, 1000, 2000, 3000],
    }
    series.update(frm)
    frm = series.frame()
    expected_a = asarray([0, 1000, 2000, 3000, 400], dtype=dtype)
    if col_type == "str":
        expected_b = ["", "1", "2", "3", ""]
    else:
        expected_b = asarray([0, 1, 2, 3, 0], dtype=dtype)
    assert (frm["a"] == expected_a).all()
    assert (frm["b"] == expected_b).all()

    # Misaligned index must raise and index
    frm = {
        "timestamp": ["2020-02-01", "2020-02-02", "2020-03-01"],
        "a": [200, 300],
    }
    with pytest.raises(ValueError):
        series.update(frm)

@pytest.mark.parametrize("how", ["left", "right", "middle"])
def test_delete(series, how):
    if how == 'middle':
        series.delete(start=1589455904, stop=1589455904)
        assert all(series.frame()['value'] == [3.3, 5.5])
    elif how == 'left':
        series.delete(start=0, stop=1589455904)
        assert all(series.frame()['value'] == [5.5])
    else:
        series.delete(start=1589455904, stop=1589455906)
        assert all(series.frame()['value'] == [3.3])


def test_tail(series, empty_series):
    frm = empty_series.tail(1)
    assert len(frm) == 0

    frm = series.tail(1, start=1589455905 + 1)
    assert len(frm) == 0

    frm = series.tail(1)
    assert len(frm) == 1
    assert frm['value'][0] == 5.5

    frm = series.tail(2)
    assert len(frm) == 2
    assert all(frm['value'] == [4.4, 5.5])

    frm = series.tail(10)
    assert len(frm) == 3
    assert all(frm['value'] == [3.3, 4.4, 5.5])

    # append some data
    series.write({
    "timestamp": [1589455906, 1589455907, 1589455908],
    "value": [6, 7, 8],
    })

    frm = series.tail(1)
    assert len(frm) == 1
    assert frm['value'][0] == 8

    frm = series.tail(4)
    assert len(frm) == 4
    assert all(frm['value'] == [5.5, 6, 7, 8])

    frm = series.tail(10)
    assert len(frm) == 6
    assert all(frm['value'] == [3.3, 4.4, 5.5, 6, 7, 8])

    frm = series.tail(10, start=1589455904)
    assert len(frm) == 5
    assert all(frm['value'] == [4.4, 5.5, 6, 7, 8])

    frm = series.tail(10, stop=1589455908)
    assert len(frm) == 5
    assert all(frm['value'] == [3.3, 4.4, 5.5, 6, 7])

    frm = series.tail(10, limit=2)
    assert len(frm) == 2
    assert all(frm['value'] == [3.3, 4.4])

    frm = series.tail(10, limit=2, offset=2)
    assert len(frm) == 2
    assert all(frm['value'] == [5.5, 6])


def test_bool(series):
    clc = series.collection
    assert bool(series)
    assert not bool(clc / 'i-do-not-exist')

# def test_partition(repo):
#     schema = Schema(timestamp="timestamp*", value="float")
#     clct = repo.create_collection(schema, "timeseries")
#     series = clct / "_"

#     # Start with a low-density frame and add higher density frames
#     deltas = [
#         timedelta(days=1),
#         timedelta(hours=1),
#         timedelta(minutes=1),
#         timedelta(seconds=1),
#         timedelta(milliseconds=100),
#     ]
#     partitions = [None, None, "Y", "W", "D"]
#     for delta, partition in zip(deltas, partitions):
#         ts = drange("2020-01-01", "2020-01-10", delta)
#         frm = {
#             "timestamp": ts,
#             "value": arange(len(ts)),
#         }
#         series.write(frm)
#         itv = series.interval()
#         assert itv == partition
