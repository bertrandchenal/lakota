from time import sleep

import pytest

from lakota import Frame, Repo, Schema
from lakota.utils import drange

schema = Schema(
    """
timestamp int*
value float
"""
)


def test_pull(threaded):
    c_label = "a_collection"
    s_label = "a_series"
    remote_repo = Repo()
    remote_coll = remote_repo.create_collection(c_label)
    remote_coll.create_series(schema, s_label)
    rseries = remote_coll.get(s_label)
    for i in range(10):
        rseries.write(
            {
                "timestamp": range(i, i + 10),
                "value": range(i + 100, i + 110),
            }
        )
    expected = rseries.frame()

    # Test pull
    local_repo = Repo()
    local_coll = local_repo + c_label
    local_coll.pull(remote_coll)
    lseries = local_coll / s_label
    assert lseries.frame() == expected

    # Test push
    other_repo = Repo()
    other_coll = other_repo + c_label
    remote_coll.push(other_coll)
    oseries = other_coll / s_label
    assert oseries.frame() == expected

    # Test with existing series
    local_repo = Repo()
    local_coll = local_repo.create_collection(c_label)
    local_coll.pull(remote_coll)
    lseries = other_repo / c_label / s_label
    assert oseries.frame() == expected

    # Test with existing series with existing data
    local_repo = Repo()
    local_coll = local_repo + c_label
    lseries = local_coll.create_series(schema, s_label)
    frm = Frame(
        schema,
        {
            "timestamp": range(0, 20),
            "value": range(10, 20),
        },
    )
    lseries.write(frm)
    local_coll.pull(remote_coll, s_label)
    assert lseries.frame() == frm

    # Test with existing series with other schema
    local_repo = Repo()
    other_schema = Schema(["timestamp int*", "value int"])
    local_coll = local_repo + c_label
    lseries = local_coll + other_schema @ s_label

    with pytest.raises(ValueError):
        local_repo.pull(remote_repo)


@pytest.mark.parametrize("squash", [False, True])
def test_label_delete_push(squash):
    labels = list("abcd")
    local_repo = Repo()
    local_coll = local_repo + "a_collection"
    remote_repo = Repo()
    remote_coll = remote_repo + "a_collection"

    # Create some labels and push them
    local_coll.create_series(schema, *labels)
    local_coll.push(remote_coll)
    if squash:
        remote_coll.squash()
    assert list(local_coll) == labels
    assert list(remote_coll) == labels

    # Delete one local label and push again
    local_coll.delete("c")
    local_coll.push(remote_coll)
    if squash:
        remote_coll.squash()
    else:
        remote_coll.refresh()
    assert list(remote_coll) == list("abd")
    assert list(local_coll) == list("abd")

    # Delete one remote label and pull
    sleep(0.1)  # Needed to avoid concurrent writes
    remote_coll.delete("d")
    local_coll.pull(remote_coll)
    if squash:
        local_coll.squash()
    else:
        local_coll.refresh()
    assert list(remote_coll) == list("ab")
    assert list(local_coll) == list("ab")


@pytest.mark.parametrize("squash", [True, False])
def test_series_push(squash):
    label = "LABEL"
    local_repo = Repo()
    local_coll = local_repo + "a_collection"
    remote_repo = Repo()
    remote_coll = remote_repo + "a_collection"
    series = local_coll.create_series(schema, label)

    months = list(range(1, 12))
    for start, stop in zip(months[:-1], months[1:]):
        ts = drange(f"2020-{start}-01", f"2020-{stop}-01", days=1)
        values = [start] * len(ts)
        series.write({"timestamp": ts, "value": values})

    local_coll.push(remote_coll, label)
