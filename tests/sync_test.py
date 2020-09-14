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


def test_pull():
    label = "LABEL"
    remote_repo = Repo()
    remote_repo.create(schema, label)
    rseries = remote_repo.get(label)
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
    local_repo.pull(remote_repo, label)
    lseries = local_repo.get(label)
    assert lseries.frame() == expected

    # Test push
    other_repo = Repo()
    remote_repo.push(other_repo, label)
    oseries = other_repo.get(label)
    assert oseries.frame() == expected

    # Test with existing series
    local_repo = Repo()
    local_repo.create(schema, label)
    local_repo.pull(remote_repo, label)
    lseries = other_repo.get(label)
    assert oseries.frame() == expected

    # Test with existing series with existing data
    local_repo = Repo()
    lseries = local_repo.create(schema, label)
    frm = Frame(
        schema,
        {
            "timestamp": range(0, 20),
            "value": range(10, 20),
        },
    )
    lseries.write(frm)
    local_repo.pull(remote_repo, label)
    assert lseries.frame() == frm

    # Test with existing series with other schema
    local_repo = Repo()
    other_schema = Schema(["timestamp int*", "value int"])
    lseries = local_repo.create(other_schema, label)
    with pytest.raises(ValueError):
        local_repo.pull(remote_repo, label)


@pytest.mark.parametrize("squash", [False, True])
def test_label_delete_push(squash):
    labels = list("abcd")
    local_repo = Repo()
    remote_repo = Repo()

    # Create some labels and push them
    local_repo.create(schema, *labels)
    local_repo.push(remote_repo)
    if squash:
        remote_repo.squash()
    assert all(local_repo.search()["label"] == labels)
    assert all(remote_repo.search()["label"] == labels)

    # Delete one local label and push again
    local_repo.delete("c")
    local_repo.push(remote_repo)
    if squash:
        remote_repo.squash()
    else:
        remote_repo.refresh()
    assert all(remote_repo.search()["label"] == list("abd"))
    assert all(local_repo.search()["label"] == list("abd"))

    # Delete one remote label and pull
    sleep(0.1)  # Needed to avoid concurrent writes
    remote_repo.delete("d")
    local_repo.pull(remote_repo)
    if squash:
        local_repo.label_series.squash()
    else:
        local_repo.refresh()
    assert all(remote_repo.search()["label"] == list("ab"))
    assert all(local_repo.search()["label"] == list("ab"))


@pytest.mark.parametrize("squash", [True, False])
def test_series_push(squash):
    label = "LABEL"
    local_repo = Repo()
    remote_repo = Repo()
    series = local_repo.create(schema, label)

    months = list(range(1, 12))
    for start, stop in zip(months[:-1], months[1:]):
        ts = drange(f"2020-{start}-01", f"2020-{stop}-01", days=1)
        values = [start] * len(ts)
        series.write({"timestamp": ts, "value": values})

    local_repo.push(remote_repo, label)
