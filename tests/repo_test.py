from time import sleep

import pytest

from lakota import Repo, Schema
from lakota.repo import LABEL_RE
from lakota.utils import chunky

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
    repo = Repo(pod=pod)
    repo.create(schema, *labels)

    # Test that we can get back those series
    for label in labels:
        series = repo.get(label)
        assert series.schema == schema

    # Same after packing
    repo.label_series.changelog.pack()
    for label in labels:
        series = repo.get(label)
        assert series.schema == schema


@pytest.mark.parametrize("squash", [True, False])
def test_create_labels_chunks(pod, squash):
    """
    Create all labels in chunks
    """
    repo = Repo(pod=pod)
    for label_chunk in chunky(labels, 3):
        repo.create(schema, *label_chunk)

    # Test that we can get back those series
    for label in labels:
        series = repo.get(label)
        assert series.schema == schema

    # Same after packing
    repo.label_series.changelog.pack()
    if squash:
        repo.squash()
    for label in labels:
        series = repo.get(label)
        assert series.schema == schema

    # Same after sqash
    for label in labels:
        series = repo.get(label)
        assert series.schema == schema


@pytest.mark.parametrize("squash", [True, False])
def test_delete(pod, squash):
    repo = Repo(pod=pod)
    repo.create(schema, *labels)
    expected = sorted(labels)
    assert list(repo.search()["label"]) == expected

    # Remove one label and check result
    sleep(0.01)
    repo.delete("seven")
    if squash:
        repo.squash()
    expected = [l for l in expected if l != "seven"]
    assert list(repo.search()["label"]) == expected

    # Remove two labels and check result
    repo.delete("nine", "zero")
    if squash:
        repo.squash()
    expected = [l for l in expected if l not in ("nine", "zero", "seven")]
    assert list(repo.search()["label"]) == expected

    # Same after sqash
    repo.squash()
    assert list(repo.search()["label"]) == expected


def test_gc(pod):
    repo = Repo(pod=pod)
    repo.create(schema, "label_a", "label_b")
    for offset, label in enumerate(("label_a", "label_b")):
        series = repo.get(label)
        for i in range(offset, offset + 10):
            series.write(
                {
                    "timestamp": range(i, i + 10),
                    "value": range(i + 100, i + 110),
                }
            )

    # Squash label_a
    series = repo.get("label_a")
    series.squash()

    # Launch garbage collection
    count = repo.gc()

    # Count must be 2 because the two series are identical except for
    # two data frames (the last write is offseted and contains two
    # columns)
    assert count == 2


def test_label_regexp():
    ok = ["abc", "abc-abc-123", "abc_abc-123.45"]
    for label in ok:
        match = LABEL_RE.match(label)
        assert match is not None
        match = LABEL_RE.match(label.upper())
        assert match is not None

    not_ok = ["", "abc+abc", "$", "é"]
    for label in not_ok:
        match = LABEL_RE.match(label)
        assert match is None
