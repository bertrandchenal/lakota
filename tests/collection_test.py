from numpy import arange

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


# @pytest.mark.parametrize("archive", [False, True])
# def test_squash(archive):
#     repo = Repo()
#     other_frame = frame.copy()
#     other_frame["value"] = [1, 2, 3]
#     temperature = repo.create_collection(schema, "temperature")
#     assert temperature.squash(archive=archive) is None

#     temp_bru = temperature / "Brussels"
#     temp_bru.write(other_frame)

#     # Capture changelog state
#     prev_commits = list(temperature.changelog)
#     assert len(prev_commits) == 1

#     # Squash
#     new_commit = temperature.squash(archive=archive)
#     # New commit should have the same digests
#     old_ci = Revision.from_path(prev_commits[0])
#     assert old_ci.digests == new_commit.digests
#     assert len(list(temperature.changelog)) == 1

#     temp_bru.write(frame)
#     temp_ory = temperature / "Paris"
#     temp_ory.write(frame)

#     # Squash collection
#     temperature.squash(archive=archive)
#     assert len(list(temperature.changelog)) == 1
#     if archive:
#         archive_temperature = repo.collection("temperature", mode="archive")
#         assert len(list(archive_temperature.changelog)) > 1

#     # Read data back
#     assert list(temperature) == ["Brussels", "Paris"]
#     for label in ("Brussels", "Paris"):
#         series = temperature / label
#         assert len(list(series.revisions())) == 1


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
    assert len(set(l.child for l in leafs)) == 1

    # Check no data is lost
    fr = bxl.frame()
    assert all(fr["value"] == arange(20))
