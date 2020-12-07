from time import sleep

import pytest

from lakota import Repo, Schema
from lakota.utils import chunky

labels = "zero one two three four five six seven eight nine".split()
schema = Schema(
    """
timestamp int *
value float
"""
)


def test_create_collections(pod):
    """
    Create all labels in one go
    """
    repo = Repo(pod=pod)
    repo.create_collection(schema, *labels)

    # Test that we can get back those series
    for label in labels:
        collection = repo / label
        assert collection.label == label

    # Same after merge
    repo.merge()
    for label in labels:
        coll = repo / label
        assert coll.label == label

    # Test double creation
    repo.create_collection(schema, *labels)
    assert sorted(repo.ls()) == sorted(labels)


@pytest.mark.parametrize("merge", [True, False])
def test_create_labels_chunks(pod, merge):
    """
    Create all labels in chunks
    """
    repo = Repo(pod=pod)
    for label_chunk in chunky(labels, 3):
        repo.create_collection(schema, *label_chunk)

    # Test that we can get back those series
    for label in labels:
        coll = repo / label
        assert coll.label == label

    # Same after merge
    if merge:
        repo.merge()
    for label in labels:
        coll = repo / label
        assert coll.label == label


@pytest.mark.parametrize("merge", [False, True])
def test_delete(pod, merge):
    repo = Repo(pod=pod)
    repo.create_collection(schema, *labels)
    expected = sorted(labels)
    assert list(repo) == expected

    # Remove one label and check result
    sleep(0.01)
    repo.delete("seven")
    if merge:
        repo.merge()
    expected = [l for l in expected if l != "seven"]
    assert list(repo) == expected

    # Remove two labels and check result
    repo.delete("nine", "zero")
    if merge:
        repo.merge()
    expected = [l for l in expected if l not in ("nine", "zero", "seven")]
    assert list(repo) == expected

    # Same after sqash
    repo.merge()
    assert list(repo) == expected


@pytest.mark.parametrize("merge", [True, False])
def test_delete_and_recreate(pod, merge):
    repo = Repo(pod=pod)
    clct = repo.create_collection(schema, "test_coll")
    series = clct / "test_series"
    series.write(
        {
            "timestamp": [1, 2, 3],
            "value": [1, 2, 3],
        }
    )

    # Delete & re-create
    repo.delete("test_coll")
    if merge:
        repo.merge()
    clct = repo.create_collection(schema, "test_coll")
    assert list(clct) == []


def test_label_regexp():
    repo = Repo()
    ok = ["abc", "abc-abc-123", "abc_abc-123.45", "abc+abc", "$", "é"]
    for label in ok:
        repo.create_collection(schema, label)
        repo.create_collection(schema, label.upper())

    not_ok = ["", "\t", "\n"]
    for label in not_ok:
        with pytest.raises(ValueError):
            repo.create_collection(schema, label)
        with pytest.raises(ValueError):
            repo.create_collection(schema, label + " ")


def test_gc(pod):
    repo = Repo(pod=pod)
    coll = repo.create_collection(schema, "a_collection")
    for offset, label in enumerate(("label_a", "label_b")):
        series = coll / label
        for i in range(offset, offset + 10):
            series.write(
                {
                    "timestamp": range(i, i + 10),
                    "value": range(i + 100, i + 110),
                }
            )

    # Merge label_a
    coll = repo / "a_collection"
    coll.merge()

    # Launch garbage collection
    # TODO implement and test squash
    count = repo.gc()
    assert count == 0

    # Read back data
    coll = repo / "a_collection"
    assert list(coll.ls()) == ["label_a", "label_b"]
