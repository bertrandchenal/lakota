from itertools import product

import pytest

from lakota import Repo, Schema
from lakota.pod import MemPOD
from lakota.utils import chunky

LABELS = "zero one two three four five six seven eight nine".split()
SCHEMA = Schema(timestamp="int *", value="float")


@pytest.fixture
def repo():
    return Repo(pod=MemPOD("."))


@pytest.mark.parametrize("squash", [True, False])
def test_create_collections(repo, squash):
    """
    Create all labels in one go
    """

    base_labels = ["b", "c", "e"]
    repo.create_collection(SCHEMA, *base_labels)

    # Test that we can get back those series
    for label in base_labels:
        collection = repo / label
        assert collection.label == label

    # Test double creation
    repo.create_collection(SCHEMA, *base_labels)
    assert sorted(repo.ls()) == sorted(base_labels)

    # Add 'a' (first), 'f' (last) and 'd' (middle)
    repo.create_collection(SCHEMA, "a")
    expected = list("abce")
    assert repo.ls() == expected

    repo.create_collection(SCHEMA, "f")
    expected = list("abcef")
    assert repo.ls() == expected

    if squash:
        repo.registry.squash()

    repo.create_collection(SCHEMA, "d")
    expected = list("abcdef")
    assert repo.ls() == expected


@pytest.mark.parametrize("merge", [True, False])
def test_create_labels_chunks(repo, merge):
    """
    Create all labels in chunks
    """
    for label_chunk in chunky(LABELS, 3):
        repo.create_collection(SCHEMA, *label_chunk)

    # Test that we can get back those series
    for label in LABELS:
        coll = repo / label
        assert coll.label == label

    # Same after merge
    if merge:
        repo.merge()
    for label in LABELS:
        coll = repo / label
        assert coll.label == label


@pytest.mark.parametrize(
    "squash,once,to_delete",
    product(
        [True, False],
        [True, False],
        [["eight"], ["zero"], ["eight", "zero"], ["seven"], ["foobar"]],
    ),
)
def test_delete(repo, squash, once, to_delete):
    if once:
        repo.create_collection(SCHEMA, *LABELS)
    else:
        for label in LABELS:
            repo.create_collection(SCHEMA, label)
    expected = sorted(LABELS)
    assert repo.ls() == expected

    # Remove one or more label and check result
    repo.delete(*to_delete)
    if squash:
        repo.registry.squash()
    expected = [l for l in expected if l not in to_delete]
    assert repo.ls() == expected
    if squash:
        repo.registry.squash()
    assert repo.ls() == expected


@pytest.mark.parametrize("merge", [True, False])
def test_delete_and_recreate(repo, merge):
    clct = repo.create_collection(SCHEMA, "test_coll")
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
    clct = repo.create_collection(SCHEMA, "test_coll")
    assert list(clct) == []


def test_label_regexp():
    repo = Repo()
    ok = ["abc", "abc-abc-123", "abc_abc-123.45", "abc+abc", "$", "é"]
    for label in ok:
        repo.create_collection(SCHEMA, label)
        repo.create_collection(SCHEMA, label.upper())

    not_ok = ["", "\t", "\n"]
    for label in not_ok:
        with pytest.raises(ValueError):
            repo.create_collection(SCHEMA, label)
        with pytest.raises(ValueError):
            repo.create_collection(SCHEMA, label + " ")


@pytest.mark.parametrize("large", [True, False])
def test_gc(repo, large):
    # Because we auto-embed small arrays in commit, we have to test
    # both small and big arrays.

    coll = repo.create_collection(SCHEMA, "a_collection")
    size = 100_000 if large else 10

    for offset, label in enumerate(("label_a", "label_b")):
        series = coll / label
        for i in range(offset, offset + 10):
            series.write(
                {
                    "timestamp": range(i, i + size),
                    "value": range(i + 100, i + 100 + size),
                }
            )

    # Merge label_a
    coll = repo / "a_collection"
    coll.merge()

    # Launch garbage collection
    count = repo.gc()
    assert count == 0

    # Read back data
    coll = repo / "a_collection"
    assert coll.ls() == ["label_a", "label_b"]


def test_refresh():
    pod = MemPOD(".")
    repo = Repo(pod=pod)

    repo.create_collection(SCHEMA, "collection")
    assert repo.ls() == ["collection"]
    repo2 = Repo(pod=pod)
    repo2.delete("collection")
    # repo is out of sync
    assert repo.ls() == ["collection"]
    # refresh slove ths
    repo.refresh()
    assert repo.ls() == []


def test_rename(repo):
    frm = {
        "timestamp": [1, 2, 3],
        "value": [1, 2, 3],
    }
    # Rename collection
    repo.create_collection(SCHEMA, "A", "B", "C")
    srs = repo / "A" / "a"
    srs.write(frm)
    repo.rename("A", "D")

    # Make sure series are still there
    srs = repo / "D" / "a"
    assert srs.frame() == frm

    # Rename to an existing label is not supported
    assert repo.ls() == ["B", "C", "D"]
    with pytest.raises(ValueError):
        repo.rename("B", "C")
