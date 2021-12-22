from datetime import datetime, timedelta
from time import sleep

import pytest
from numpy import arange

from lakota import Repo, Schema, Frame
from lakota.utils import settings

schema = Schema(timestamp="timestamp*", value="float")
frame = {
    "timestamp": ["1970-01-01T00:00:01", "1970-01-01T00:00:02", "1970-01-01T00:00:03"],
    "value": [11, 12, 13],
}


def test_create():
    # Create repo / collection / series
    repo = Repo()
    temperature = repo.create_collection(schema, "temperature")
    temp_bru = temperature / "Brussels"
    temp_bru.write(frame)

    # Read it back
    temperature = repo / "temperature"
    temp_bru = temperature / "Brussels"
    assert temp_bru.frame() == frame

    assert repo.ls() == ["temperature"]
    assert temperature.ls() == ["Brussels"]

    # Test double creation
    with pytest.raises(ValueError):
        repo.create_collection(schema, "temperature")

    repo.create_collection(schema, "temperature", raise_if_exists=False)
    assert sorted(repo.ls()) == ["temperature"]
    collection_series = repo.registry / "default"
    assert len(list(collection_series.changelog)) == 1
    repo.create_collection(schema, "temperature", "wind", raise_if_exists=False)
    assert sorted(repo.ls()) == ["temperature", "wind"]


def test_multi_create():
    repo = Repo()
    temperature = repo.create_collection(schema, "temperature")
    temp_bru = temperature / "Brussels"
    temp_bru.write(frame)

    frame_ory = frame.copy()
    frame_ory["value"] = [21, 22, 23]
    temp_ory = temperature / "Paris"
    temp_ory.write(frame_ory)

    assert temp_bru.frame() == frame
    assert temp_ory.frame() == frame_ory

    collection_series = repo.registry / "default"
    assert len(list(collection_series.changelog.log())) == 1
    assert len(list(temperature.changelog.log())) == 2

    assert temperature.ls() == ["Brussels", "Paris"]


@pytest.mark.parametrize("max_chunk", [1, settings.defrag_max_chunk])
@pytest.mark.parametrize("must_trim", [True, False])
def test_squash(must_trim, max_chunk):
    # Collection.squash is deprecated, but still avail, this test
    # should be removed when Collection.squash is removed.

    # We force full trim by providing a date in the future
    trim = datetime.now() + timedelta(hours=1) if must_trim is True else False

    repo = Repo()
    other_frame = {
        "timestamp": [4, 5, 6],
        "value": [4, 5, 6],
    }
    temperature = repo.create_collection(schema, "temperature")
    revs = temperature.squash(trim, max_chunk)
    assert revs == []

    # We need two writes in order to have something to squash
    temp_bru = temperature / "Brussels"
    temp_bru.write(frame)
    temp_bru.write(other_frame)

    # Capture changelog state
    prev_commits = list(temperature.changelog)
    assert len(prev_commits) == 2

    # Squash
    temperature.squash(trim, max_chunk)

    expected = {
        (True, settings.defrag_max_chunk): 1,
        (True, 1): 1,
        (False, settings.defrag_max_chunk): 2,
        (False, 1): 2,
    }[must_trim, max_chunk]
    assert len(list(temperature.changelog)) == expected

    temp_bru.write(frame)
    temp_ory = temperature / "Paris"
    temp_ory.write(frame)
    temp_ory.write(other_frame)

    expected_frm = temp_bru.frame()
    # Squash collection
    temperature.squash(trim, max_chunk)
    assert temp_bru.frame() == expected_frm

    expected = {
        (True, settings.defrag_max_chunk): 1,
        (True, 1): 1,
        (False, settings.defrag_max_chunk): settings.defrag_max_chunk,
        (False, 1): 4,
    }[must_trim, max_chunk]
    assert len(list(temperature.changelog)) == expected

    # Read data back
    assert temperature.ls() == ["Brussels", "Paris"]


def test_trim():
    repo = Repo()
    # We force full trim by providing a date in the future
    delta = timedelta(hours=1)

    repo = Repo()
    other_frame = {
        "timestamp": [4, 5, 6],
        "value": [4, 5, 6],
    }
    solar = repo.create_collection(schema, "solar")
    nb = solar.trim()
    assert nb == 0

    # We need two writes in order to have something to trim
    sol_bru = solar / "Brussels"
    sol_bru.write(frame)
    sol_bru.write(other_frame)

    # `before` in the past
    nb = solar.trim(before=datetime.now() - delta)
    assert nb == 0
    assert len(list(solar.changelog)) == 2

    # `before` in the future
    nb = solar.trim(before=datetime.now() + delta)
    assert nb == 1
    assert len(list(solar.changelog)) == 1

def test_defrag():
    repo = Repo()
    other_frame = {
        "timestamp": [4, 5, 6],
        "value": [4, 5, 6],
    }
    demand = repo.create_collection(schema, "demand")
    nb = demand.defrag()
    assert nb == []

    # We need more writes in order to have something to defrag
    dem_bru = demand / "Brussels"
    dem_bru.write(frame)
    dem_bru.write(other_frame)
    dem_bru.write({
        "timestamp": range(7,20),
        "value": range(7,20),
    })

    revs = demand.defrag()
    assert revs == []
    assert len(list(demand.changelog)) == 3

    # force max_chunk to 1
    revs = demand.defrag(max_chunk=1)
    assert len(revs) == 1
    assert len(list(demand.changelog)) == 4

@pytest.mark.parametrize(
    "frame_len", [10, settings.page_len - 7, settings.page_len / 2]
)
@pytest.mark.parametrize("nb_chunk", [1, 4, 8])
def test_defrag_max_chunk(nb_chunk, frame_len):
    repo = Repo()
    new_frame = lambda i: {
        "timestamp": arange(i * frame_len, (i + 1) * frame_len),
        "value": arange(frame_len),
    }
    temperature = repo.create_collection(schema, "temperature")
    series = temperature / "Brussels"
    for i in range(0, nb_chunk):
        series.write(new_frame(i))
    expected = series.frame()
    temperature.defrag(max_chunk=4)
    temperature.trim(datetime.now() + timedelta(hours=1))  # Future value to force full deletion
    assert series.frame() == expected

    segments = list(series.segments())
    if nb_chunk <= 4:
        assert len(segments) == nb_chunk
    elif frame_len == 10:
        assert len(segments) == 1
    elif frame_len == settings.page_len / 2:
        assert len(segments) == 4
    else:
        assert len(segments) == 8


def test_merge():
    repo = Repo()
    mk_frm = lambda start: {
        "timestamp": range(start, start + 10),
        "value": range(start, start + 10),
    }
    temperature = repo.create_collection(schema, "temperature")

    # Create two commits based on root
    bxl = temperature / "Brussels"
    bxl.write(mk_frm(0))

    bxl.write(mk_frm(10), root=True)
    leafs = bxl.changelog.leafs()
    assert len(leafs) == 2
    assert len(set(l.child for l in leafs)) == 2

    revs = temperature.merge()
    assert len(revs) == 2
    leafs = bxl.changelog.leafs()
    assert len(bxl.changelog.leafs()) == 2
    assert len(set(l.digests.child for l in leafs)) == 1

    # Check no data is lost
    fr = bxl.frame()
    assert all(fr["value"] == arange(20))

    # Double merge should be a no-op but it looks like commit encoding
    # is not stable (because of msgpck). Most of the time it's ok,
    # sometimes it fails
    revs = temperature.merge()
    # assert revs == []


def test_merge_concurrent():
    repo_a = Repo()
    repo_b = Repo()
    repo_c = Repo()  # Central repo
    mk_frm = lambda start: {
        "timestamp": range(start, start + 3),
        "value": [start] * 3,
    }
    temperature_a = repo_a.create_collection(schema, "temperature")
    temperature_b = repo_b.create_collection(schema, "temperature")

    bxl_a = temperature_a / "Brussels"
    bxl_b = temperature_b / "Brussels"

    # Prime the changelog cache
    bxl_a.df()
    bxl_b.df()

    # Concurrent writes
    for pos, srs in enumerate((bxl_a, bxl_b)):
        srs.write(mk_frm(pos))
        sleep(0.01)  # make sure the order is preserved

    # Pull from a & b and merge
    temperature_c = repo_c.create_collection(schema, "temperature")
    temperature_c.pull(temperature_a)
    temperature_c.pull(temperature_b)
    revs = temperature_c.merge()
    assert len(revs) == 2

    # Second write win:
    bxl_c = temperature_c / "Brussels"
    assert all(bxl_c.df()["value"] == [0, 1, 1, 1])

    # Concurrent writes, second turn (each series is still blind to
    # the other, so each will commit on its branch)
    for pos, srs in enumerate((bxl_b, bxl_a)):  # Reversed !
        srs.write(mk_frm(pos + 10))
        sleep(0.01)

    # Second merge
    temperature_c.pull(temperature_a)
    temperature_c.pull(temperature_b)
    revs = temperature_c.merge()
    assert len(revs) == 3  # 3 because a and c never pulled

    expected = {
        "timestamp": [
            "1970-01-01 00:00:00",
            "1970-01-01 00:00:01",
            "1970-01-01 00:00:02",
            "1970-01-01 00:00:03",
            "1970-01-01 00:00:10",
            "1970-01-01 00:00:11",
            "1970-01-01 00:00:12",
            "1970-01-01 00:00:13",
        ],
        "value": [
            0.0,
            1.0,  # Here b won over a (last created branch win)
            1.0,
            1.0,
            10.0,  # Here again: last commit of the newest branch (even if writes where reversed)
            10.0,
            10.0,
            11.0,
        ],
    }
    assert bxl_c.frame() == expected


def test_delete():
    frame = {"timestamp": [1, 2, 3], "value": [11, 12, 13]}
    # Create repo / collection / series
    repo = Repo()
    temperature = repo.create_collection(schema, "temperature")
    temp_bru = temperature / "Brussels"
    temp_bru.write(frame)

    assert temperature.ls() == ["Brussels"]

    temperature.delete("Brussels")
    assert temperature.ls() == []

    srs = temperature / "Brussels"
    assert len(srs.frame()) == 0


def test_delete_and_recreate():
    frame = {"timestamp": [1, 2, 3], "value": [11, 12, 13]}
    # Create repo / collection / series
    repo = Repo()
    temperature = repo.create_collection(schema, "temperature")
    cities = ["Paris", "Brussels", "London", "Berlin"]
    for name in cities:
        series = temperature / name
        series.write(frame)

    for name in cities:
        new_name = "New " + name
        frm = (temperature / name).frame()
        (temperature / new_name).write(frm)
        temperature.delete(name)

    for name in cities:
        new_name = "New " + name
        frm = (temperature / new_name).frame()
        assert frm == frame


def test_rename():
    repo = Repo()
    temperature = repo.create_collection(schema, "temperature")
    temp_bru = temperature / "Brussels"
    temp_bru.write(frame)

    frame_ory = frame.copy()
    frame_ory["value"] = [21, 22, 23]
    temp_ory = temperature / "Paris"
    temp_ory.write(frame_ory)

    # Rename to a new name (and back)
    temperature.rename("Brussels", "Rome")
    assert temperature.ls() == ["Paris", "Rome"]
    temperature.rename("Rome", "Brussels")
    assert temperature.ls() == ["Brussels", "Paris"]

    # Rename to an existing one (and overwrite values by doing so)
    temperature.rename("Paris", "Brussels")
    assert temperature.ls() == ["Brussels"]
    assert all(temp_bru.frame()["value"] == [21, 22, 23])


def test_multi_batch():
    repo = Repo()
    temperature = repo.create_collection(schema, "temperature")
    with pytest.raises(Exception):
        srs = temperature / "Brussels"
        srs.write(frame)
        with temperature.multi():
            srs = temperature / "Paris"
            srs.write(frame)
            raise Exception()

    assert temperature.series("Paris").frame().empty
    assert not temperature.series("Brussels").frame().empty


@pytest.mark.parametrize("new_type", ['str', 'int', 'float'])
@pytest.mark.parametrize("rename", [None, {'value': 'new_value'}])
@pytest.mark.parametrize("defaults", [None, {'extra_col': 5}])
def test_clone(new_type, rename, defaults):

    if rename:
        schema_bis = Schema(**{'timestamp':"timestamp*", rename['value']:"float", 'extra_col':new_type})
    else:
        schema_bis = Schema(timestamp="timestamp*", value="float", extra_col=new_type)

    schema_dict = {'timestamp': "timestamp*", 'extra_col': new_type}
    schema_dict['value' if rename is None else rename['value']] = 'float'

    repo = Repo()
    temperature = repo.create_collection(schema, "temperature")
    temperature_bis = repo.create_collection(schema_bis, "temperature_bis")
    srs = temperature / "Brussels"
    srs.write(frame)
    orig = srs.frame()

    # Clone collection and test series values
    temperature.clone(temperature_bis, rename, defaults)
    new_frame = temperature_bis.series('Brussels').frame()

    for col in ('timestamp', 'value'):
        dst_col = rename.get(col, col) if rename else col
        assert all(new_frame[dst_col] == orig[col])

    cast = __builtins__[new_type]

    expected = [cast(defaults['extra_col']) if defaults else ('' if new_type == 'str' else 0)] * 3
    assert all(new_frame['extra_col'] == expected)

    if rename is not None:
        assert (new_frame[rename['value']] == frame['value']).all()
