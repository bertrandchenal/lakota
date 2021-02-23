import pytest
from numpy import arange

from lakota.changelog import phi
from lakota.repo import Repo, Schema

schema = Schema(["timestamp timestamp*", "value float"])
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


@pytest.mark.parametrize("fast", [True, False])
def test_squash(fast):
    repo = Repo()
    other_frame = {
        "timestamp": [4, 5, 6],
        "value": [4, 5, 6],
    }
    temperature = repo.create_collection(schema, "temperature")
    revs = temperature.squash(fast=fast)
    assert revs == []

    # We need two writes in order to have something to squash
    temp_bru = temperature / "Brussels"
    temp_bru.write(frame)
    temp_bru.write(other_frame)

    # Capture changelog state
    prev_commits = list(temperature.changelog)
    assert len(prev_commits) == 2

    # Squash
    if fast:
        temperature.squash(fast=True)
    else:
        (new_commit,) = temperature.squash(fast=False)
        # Non-fast commit are based on root
        assert new_commit.parent == phi

    assert len(list(temperature.changelog)) == 1

    temp_bru.write(frame)
    temp_ory = temperature / "Paris"
    temp_ory.write(frame)
    temp_ory.write(other_frame)

    # Squash collection
    temperature.squash(fast=fast)
    assert len(list(temperature.changelog)) == 1

    # Read data back
    assert list(temperature) == ["Brussels", "Paris"]
    assert len(list(temperature.changelog.log())) == 1


# TODO implement partial squash (to keep recent history), and change frame or other_frame to have an overlap, and make sure the correct one "wins"


def test_merge():
    repo = Repo()
    mk_frm = lambda start: {
        "timestamp": range(start, start + 10),
        "value": range(start, start + 10),
    }
    temperature = repo.create_collection(schema, "temperature")

    # Create separate instances of the same series
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
