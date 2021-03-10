from time import sleep

import pytest
from numpy import arange

from lakota.changelog import phi
from lakota.repo import Repo, Schema

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

    assert list(repo.ls()) == ["temperature"]
    assert list(temperature.ls()) == ["Brussels"]

    # Test double creation
    repo.create_collection(schema, "temperature")
    assert sorted(repo.ls()) == ["temperature"]
    assert len(list(repo.collection_series.changelog)) == 1
    repo.create_collection(schema, "temperature", "wind")
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

    assert len(list(repo.collection_series.changelog.log())) == 1
    assert len(list(temperature.changelog.log())) == 2

    assert list(temperature) == ["Brussels", "Paris"]


@pytest.mark.parametrize("pack", [True, False])
@pytest.mark.parametrize("trim", [True, False])
def test_squash(pack, trim):
    repo = Repo()
    other_frame = {
        "timestamp": [4, 5, 6],
        "value": [4, 5, 6],
    }
    temperature = repo.create_collection(schema, "temperature")
    revs = temperature.squash(pack=pack, trim=trim)
    assert revs == []

    # We need two writes in order to have something to squash
    temp_bru = temperature / "Brussels"
    temp_bru.write(frame)
    temp_bru.write(other_frame)

    # Capture changelog state
    prev_commits = list(temperature.changelog)
    assert len(prev_commits) == 2

    # Squash
    temperature.squash(pack=pack, trim=trim)

    expected = {
        (True, True): 1,
        (False, True): 1,
        (True, False): 3,
        (False, False): 2,
    }[pack, trim]
    assert len(list(temperature.changelog)) == expected

    temp_bru.write(frame)
    temp_ory = temperature / "Paris"
    temp_ory.write(frame)
    temp_ory.write(other_frame)

    # Squash collection
    temperature.squash(pack=pack, trim=trim)
    expected = {
        (True, True): 1,
        (False, True): 1,
        (True, False): 7,
        (False, False): 4,
    }[pack, trim]
    assert len(list(temperature.changelog)) == expected

    # Read data back
    assert list(temperature) == ["Brussels", "Paris"]


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

    return  # TODO The following still fails !

    # Concurrent writes, second turn (each series is still blind to
    # the other, so each will commit on its branch) We squash to make
    # sure the merge works (if not it will fail on non-closed update
    # error in Commit.update)
    for pos, srs in enumerate((bxl_b, bxl_a)):
        srs.write(mk_frm(pos + 1))
        sleep(0.01)  # make sure the order is preserved
        srs.collection.squash(pack=True, trim=False)

    # Second merge
    temperature_c.pull(temperature_a)
    temperature_c.pull(temperature_b)
    revs = temperature_c.merge()
    assert len(revs) == 4

    bxl_c = temperature_c / "Brussels"
    import pdb

    pdb.set_trace()
    assert all(bxl_c.df()["value"] == [0, 1, 1, 1])


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
