from itertools import islice
from time import sleep

import pytest

from lakota import Registry, Schema
from lakota.registry import LABEL_RE

labels = "zero one two three four five six seven eight nine".split()
schema = Schema(
    """
timestamp int *
value float
"""
)


def test_create_labels(pod):
    """
    Create all labels in one go
    """
    reg = Registry(pod=pod)
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


@pytest.mark.parametrize("squash", [True, False])
def test_create_labels_chunks(pod, squash):
    """
    Create all labels in chunks
    """
    it = iter(labels)
    reg = Registry(pod=pod)
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
    if squash:
        reg.squash()
    for label in labels:
        series = reg.get(label)
        assert series.schema == schema

    # Same after sqash
    for label in labels:
        series = reg.get(label)
        assert series.schema == schema


@pytest.mark.parametrize("squash", [True, False])
def test_delete(pod, squash):
    reg = Registry(pod=pod)
    reg.create(schema, *labels)
    expected = sorted(labels)
    assert list(reg.search()["label"]) == expected

    # Remove one label and check result
    sleep(0.01)
    reg.delete("seven")
    if squash:
        reg.squash()
    expected = [l for l in expected if l != "seven"]
    assert list(reg.search()["label"]) == expected

    # Remove two labels and check result
    reg.delete("nine", "zero")
    if squash:
        reg.squash()
    expected = [l for l in expected if l not in ("nine", "zero", "seven")]
    assert list(reg.search()["label"]) == expected

    # Same after sqash
    reg.squash()
    assert list(reg.search()["label"]) == expected


def test_gc(pod):
    reg = Registry(pod=pod)
    reg.create(schema, "label_a", "label_b")
    for offset, label in enumerate(("label_a", "label_b")):
        series = reg.get(label)
        for i in range(offset, offset + 10):
            series.write(
                {
                    "timestamp": range(i, i + 10),
                    "value": range(i + 100, i + 110),
                }
            )

    # Squash label_a
    series = reg.get("label_a")
    series.squash()

    # Launch garbage collection
    count = reg.gc()

    # Count must be 2 because the two series are identical except for
    # two data frames (the last write is offseted and contains two
    # columns)
    assert count == 2


def test_label_regexp():
    ok = ["abc", "abc-abc-123", "abc_abc-123"]
    for label in ok:
        match = LABEL_RE.match(label)
        assert match is not None
        match = LABEL_RE.match(label.upper())
        assert match is not None

    not_ok = ["", "abc+abc", "$", "é"]
    for label in not_ok:
        match = LABEL_RE.match(label)
        assert match is None
