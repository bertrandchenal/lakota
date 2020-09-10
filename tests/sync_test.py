from time import sleep

import pytest

from lakota import Frame, Registry, Schema
from lakota.utils import drange

schema = Schema(
    """
timestamp int*
value float
"""
)


def test_pull():
    label = "LABEL"
    remote_reg = Registry()
    remote_reg.create(schema, label)
    rseries = remote_reg.get(label)
    for i in range(10):
        rseries.write(
            {
                "timestamp": range(i, i + 10),
                "value": range(i + 100, i + 110),
            }
        )
    expected = rseries.frame()

    # Test pull
    local_reg = Registry()
    local_reg.pull(remote_reg, label)
    lseries = local_reg.get(label)
    assert lseries.frame() == expected

    # Test push
    other_reg = Registry()
    remote_reg.push(other_reg, label)
    oseries = other_reg.get(label)
    assert oseries.frame() == expected

    # Test with existing series
    local_reg = Registry()
    local_reg.create(schema, label)
    local_reg.pull(remote_reg, label)
    lseries = other_reg.get(label)
    assert oseries.frame() == expected

    # Test with existing series with existing data
    local_reg = Registry()
    lseries = local_reg.create(schema, label)
    frm = Frame(
        schema,
        {
            "timestamp": range(0, 20),
            "value": range(10, 20),
        },
    )
    lseries.write(frm)
    local_reg.pull(remote_reg, label)
    assert lseries.frame() == frm

    # Test with existing series with other schema
    local_reg = Registry()
    other_schema = Schema(["timestamp int*", "value int"])
    lseries = local_reg.create(other_schema, label)
    with pytest.raises(ValueError):
        local_reg.pull(remote_reg, label)


@pytest.mark.parametrize("squash", [False, True])
def test_label_delete_push(squash):
    labels = list("abcd")
    local_reg = Registry()
    remote_reg = Registry()

    # Create some labels and push them
    local_reg.create(schema, *labels)
    local_reg.push(remote_reg)
    if squash:
        remote_reg.squash()
    assert all(local_reg.search()["label"] == labels)
    assert all(remote_reg.search()["label"] == labels)

    # Delete one local label and push again
    local_reg.delete("c")
    local_reg.push(remote_reg)
    if squash:
        remote_reg.squash()
    else:
        remote_reg.refresh()
    assert all(remote_reg.search()["label"] == list("abd"))
    assert all(local_reg.search()["label"] == list("abd"))

    # Delete one remote label and pull
    sleep(0.1)  # Needed to avoid concurrent writes
    remote_reg.delete("d")
    local_reg.pull(remote_reg)
    if squash:
        local_reg.label_series.squash()
    else:
        local_reg.refresh()
    assert all(remote_reg.search()["label"] == list("ab"))
    assert all(local_reg.search()["label"] == list("ab"))


@pytest.mark.parametrize("squash", [True, False])
def test_series_push(squash):
    label = "LABEL"
    local_reg = Registry()
    remote_reg = Registry()
    series = local_reg.create(schema, label)

    months = list(range(1, 12))
    for start, stop in zip(months[:-1], months[1:]):
        ts = drange(f"2020-{start}-01", f"2020-{stop}-01", days=1)
        values = [start] * len(ts)
        series.write({"timestamp": ts, "value": values})

    local_reg.push(remote_reg, label)
