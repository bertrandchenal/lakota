from itertools import islice
from uuid import uuid4

import pytest

from baltic import Registry, Schema


def test_create_labels(pod):
    """
    Create all labels in one go
    """

    labels = [str(uuid4()) for _ in range(10)]

    # Series.write will prevent un-sorted writes
    with pytest.raises(AssertionError):
        reg = Registry(pod=pod)
        schema = Schema(["timestamp:int", "value:float"])
        reg.create(schema, *labels)

    # Same but with sorted labels
    labels = sorted(labels)
    reg = Registry(pod=pod)
    schema = Schema(["timestamp:int", "value:float"])
    reg.create(schema, *labels)


    # Test that we can get back those series
    for label in labels:
        series = reg.get(label)
        assert series.schema == schema

    # Same after packing
    reg.schema_series.changelog.pack()
    for label in labels:
        series = reg.get(label)
        assert series.schema == schema


def test_create_labels_chunks(pod):
    """
    Create all labels in chunks
    """
    labels = sorted(str(uuid4()) for _ in range(10))
    it = iter(labels)
    reg = Registry(pod=pod)
    schema = Schema(["timestamp:int", "value:float"])
    while True:
        sl_labels = list(islice(it, 3))
        if not sl_labels:
            break
        reg.create(schema, *sl_labels)

    # Test that we can get back those series
    for label in labels:
        series = reg.get(label)
        assert series.schema == schema

    # Same after packing
    reg.schema_series.changelog.pack()
    for label in labels:
        series = reg.get(label)
        assert series.schema == schema

    # Same after sqash
    reg.schema_series.squash()
    for label in labels:
        series = reg.get(label)
        assert series.schema == schema


def test_clone():
    schema = Schema(["timestamp:int", "value:float"])
    label = 'LABEL'
    remote_reg = Registry()
    remote_reg.create(schema, label)
    rseries = remote_reg.get(label)
    for i in range(10):
        rseries.write({
            'timestamp': range(i, i+10),
            'value': range(i+100, i+110),
        })
    expected = rseries.read()

    local_reg = Registry()
    local_reg.clone(remote_reg, label)

    lseries = local_reg.get(label)
    assert lseries.read() == expected
