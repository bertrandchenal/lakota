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

    assert len(list(repo.revisions())) == 1
    assert len(list(temperature.revisions())) == 2
    assert len(list(temp_bru.revisions())) == 1

    assert list(temperature) == ["Brussels", "Paris"]


def test_squash():
    repo = Repo()
    other_frame = frame.copy()
    other_frame["value"] = [1, 2, 3]
    temperature = repo.create_collection(schema, "temperature")
    temp_bru = temperature / "Brussels"
    temp_bru.write(other_frame)
    temp_bru.write(frame)

    temp_ory = temperature / "Paris"
    temp_ory.write(frame)

    # Squash collection
    temperature.squash()

    # Read data back
    assert list(temperature) == ["Brussels", "Paris"]
    for label in ("Brussels", "Paris"):
        series = temperature / label
        assert len(list(series.revisions())) == 1
