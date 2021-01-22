from time import sleep

import pytest
from numpy import arange

from lakota.changelog import Revision
from lakota.repo import Repo, Schema

schema = Schema(["timestamp timestamp*", "value float"])
frame = {"timestamp": [1, 2, 3], "value": [11, 12, 13]}


def test_create():
    frame = {"timestamp": [1, 2, 3], "value": [11, 12, 13]}
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


def test_multi():
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


@pytest.mark.parametrize(
    "fast",
    [
        True,
        False,
    ],
)
def test_squash(fast):
    repo = Repo()
    other_frame = {
        "timestamp": [4, 5, 6],
        "value": [4, 5, 6],
    }
    temperature = repo.create_collection(schema, "temperature")
    assert temperature.squash(fast=fast) is None

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
        # New commit should have the same digests
        old_ci = Revision.from_path(temperature.changelog, prev_commits[1])
        assert old_ci.child == new_commit.parent

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
    sleep(0.1)
    bxl.write(mk_frm(10), root=True)
    leafs = bxl.changelog.leafs()
    assert len(leafs) == 2
    assert len(set(l.child for l in leafs)) == 2

    revs = temperature.merge()
    assert len(revs) == 2
    leafs = bxl.changelog.leafs()
    assert len(bxl.changelog.leafs()) == 2
    assert len(set(l.child for l in leafs)) == 1

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
