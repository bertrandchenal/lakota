from datetime import timedelta
from time import sleep

import pytest
from numpy import arange

from lakota import Frame, Repo, Schema
from lakota.changelog import Revision
from lakota.utils import drange, settings

schema = Schema(timestamp="int*", value="float")


@pytest.mark.parametrize("large", [True, False])
def test_pull(threaded, large):
    c_label = "a_collection"
    s_label = "a_series"
    remote_repo = Repo()
    remote_coll = remote_repo.create_collection(schema, c_label)
    rseries = remote_coll / s_label

    # Test support of both small dataset (where data is embedded in
    # commits) and large one (arrays are save on their own)
    N = 100_000 if large else 10
    for i in range(10):
        # Create 10 series of size N
        rseries.write(
            {
                "timestamp": range(i, i + N),
                "value": range(i + 100, i + 100 + N),
            }
        )
    nb_items = len(remote_repo.pod.ls())
    if large:
        assert nb_items > 2
    else:
        # for small arrays we have only two folder (one for the repo
        # registry one for the collection)
        assert nb_items == 2
    expected = rseries.frame()

    # Test pull
    local_repo = Repo()
    local_coll = local_repo.create_collection(schema, c_label)
    local_coll.pull(remote_coll)
    lseries = local_coll / s_label
    assert lseries.frame() == expected

    # Test push
    other_repo = Repo()
    other_coll = other_repo.create_collection(schema, c_label)
    remote_coll.push(other_coll)
    oseries = other_coll / s_label
    assert oseries.frame() == expected

    # Test with existing series
    local_repo = Repo()
    local_coll = local_repo.create_collection(schema, c_label)
    local_coll.pull(remote_coll)
    lseries = (
        other_repo.create_collection(schema, c_label, raise_if_exists=False) / s_label
    )
    assert oseries.frame() == expected

    # Test with existing series with existing data
    local_repo = Repo()
    local_coll = local_repo.create_collection(schema, c_label)
    lseries = local_coll / s_label
    frm = Frame(
        schema,
        {
            "timestamp": range(0, 20),
            "value": range(0, 20),
        },
    )
    lseries.write(frm)
    local_coll.pull(remote_coll)
    assert lseries.frame() == frm

    # Test with existing series with other schema
    local_repo = Repo()
    other_schema = Schema(timestamp="int*", value="int")
    local_coll = local_repo.create_collection(other_schema, c_label)
    lseries = local_coll / s_label

    with pytest.raises(ValueError):
        local_repo.pull(remote_repo)


@pytest.mark.parametrize("defrag", [False, True])
def test_label_delete_push(defrag):
    kv_schema = Schema.kv(timestamp="int*", value="float")

    labels = list("abcd")
    local_repo = Repo()
    local_clct = local_repo.create_collection(kv_schema, "a_collection")
    remote_repo = Repo()
    remote_clct = remote_repo.create_collection(kv_schema, "a_collection")

    # Write some data
    frm = {
        "timestamp": [1, 2, 3],
        "value": [1, 2, 3],
    }
    for label in labels:
        series = local_clct / label
        series.write(frm)

    # Create some labels and push them
    local_clct.push(remote_clct)
    if defrag:
        remote_clct.defrag()
    assert local_clct.ls() == labels
    assert remote_clct.ls() == labels

    # Delete one local label and push again
    local_clct.delete("c")
    local_clct.push(remote_clct)
    if defrag:
        remote_clct.merge()
        remote_clct.defrag()

    else:
        remote_clct.refresh()

    assert remote_clct.ls() == list("abd")
    assert local_clct.ls() == list("abd")

    # Delete one remote label and pull
    sleep(0.1)  # Needed to avoid concurrent writes
    remote_clct.delete("d")
    local_clct.pull(remote_clct)
    if defrag:
        local_clct.defrag()
    else:
        local_clct.refresh()
    assert remote_clct.ls() == list("ab")
    assert local_clct.ls() == list("ab")


def test_series_defrag_stability():
    label = "LABEL"
    local_repo = Repo()
    local_coll = local_repo.create_collection(schema, "a_collection")
    remote_repo = Repo()
    remote_repo.pull(local_repo)
    remote_coll = remote_repo / "a_collection"
    series = local_coll / label

    months = list(range(1, 12))
    delta = timedelta(days=1)
    for start, stop in zip(months[:-1], months[1:]):
        ts = drange(f"2020-{start:02}-01", f"2020-{stop:02}-01", delta)
        values = [start] * len(ts)
        series.write({"timestamp": ts, "value": values})

    local_coll.push(remote_coll)
    local_coll.defrag()
    remote_coll.defrag()

    local_files = local_coll.pod.walk()
    remote_files = remote_coll.pod.walk()

    local_digests = set(
        Revision.from_path(local_coll.changelog, f).digests
        for f in local_files
        if "." in f
    )
    remote_digests = set(
        Revision.from_path(remote_coll.changelog, f).digests
        for f in remote_files
        if "." in f
    )
    assert local_digests == remote_digests


@pytest.mark.parametrize("shallow", [False, True])
@pytest.mark.parametrize("direction", ["push", "pull"])
@pytest.mark.parametrize("size", [10, settings.page_len])
def test_series_shallow_pull(size, direction, shallow):
    label = "LABEL"
    local_repo = Repo()
    remote_repo = Repo()
    local_coll = local_repo.create_collection(schema, "a_collection")
    series = local_coll / label

    series.write({"timestamp": arange(size), "value": arange(size)})
    series.write({"timestamp": arange(size), "value": arange(size) * 2})

    if direction == "pull":
        remote_repo.pull(local_repo, shallow=shallow)
    else:
        local_repo.push(remote_repo, shallow=shallow)

    remote_clc = remote_repo / "a_collection"
    assert len(remote_clc.changelog.log()) == (1 if shallow else 2)

    remote_series = remote_clc / label
    expected = series.frame()
    assert remote_series.frame() == expected
