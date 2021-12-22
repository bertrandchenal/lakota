from datetime import datetime, timedelta
from itertools import product
from time import sleep

import pytest

from lakota import Repo, Schema
from lakota.pod import MemPOD
from lakota.utils import chunky, settings

LABELS = "zero one two three four five six seven eight nine".split()
SCHEMA = Schema(timestamp="int *", value="float")


@pytest.fixture
def repo():
    return Repo(pod=MemPOD("."))


@pytest.mark.parametrize("defrag", [True, False])
def test_create_collections(repo, defrag):
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
    repo.create_collection(SCHEMA, *base_labels, raise_if_exists=False)
    assert sorted(repo.ls()) == sorted(base_labels)

    # Add 'a' (first), 'f' (last) and 'd' (middle)
    repo.create_collection(SCHEMA, "a")
    expected = list("abce")
    assert repo.ls() == expected

    repo.create_collection(SCHEMA, "f")
    expected = list("abcef")
    assert repo.ls() == expected

    if defrag:
        repo.registry.defrag(max_chunk=1)

    repo.create_collection(SCHEMA, "d")
    expected = list("abcdef")
    assert repo.ls() == expected


def test_double_create_collections(repo):
    """
    Test double call to create_collection for a same label
    """

    repo.create_collection(SCHEMA, "a", "b")
    with pytest.raises(ValueError):
        repo.create_collection(SCHEMA, "b", "c")
    assert repo.ls() == ["a", "b"]

    repo.create_collection(SCHEMA, "b", "c", raise_if_exists=False)
    assert repo.ls() == ["a", "b", "c"]


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
    "defrag,once,to_delete",
    product(
        [True, False],
        [True, False],
        [["eight"], ["zero"], ["eight", "zero"], ["seven"], ["foobar"]],
    ),
)
def test_delete(repo, defrag, once, to_delete):
    if once:
        repo.create_collection(SCHEMA, *LABELS)
    else:
        for label in LABELS:
            repo.create_collection(SCHEMA, label)
    expected = sorted(LABELS)
    assert repo.ls() == expected

    # Remove one or more label and check result
    repo.delete(*to_delete)
    if defrag:
        repo.registry.defrag()
    expected = [l for l in expected if l not in to_delete]
    assert repo.ls() == expected
    for label in to_delete:
        assert repo / label is None


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
    ok = ["abc", "abc-abc-123", "abc_abc-123.45", "abc+abc", "$", "é",
          "foo bar"]
    for label in ok:
        repo.create_collection(SCHEMA, label)
        repo.create_collection(SCHEMA, label.upper(), raise_if_exists=False)

    not_ok = ["", "\t", "\n", " "]
    for label in not_ok:
        with pytest.raises(ValueError):
            repo.create_collection(SCHEMA, label)
        with pytest.raises(ValueError):
            repo.create_collection(SCHEMA, label + " ")


@pytest.mark.parametrize("large", [True, False])
def test_gc(repo, large):
    # Because we auto-embed small arrays in commit, we have to test
    # both small and big arrays.
    labels = ["label_a", "label_b"]
    coll = repo.create_collection(SCHEMA, "a_collection")
    size = 100_000 if large else 10

    for offset, label in enumerate(labels):
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

    # Launch garbage collection, no commit have beend deleted so there
    # is nothing to collect
    counts = repo.gc()
    assert counts == (0, 0)

    # Read back data
    coll = repo / "a_collection"
    assert coll.ls() == labels

    if not large:
        # Not need to test further as all data will be embedded in
        # commits
        return

    # Defrag and Trim collection (this will delete older commits) &
    # test again.
    coll.defrag()
    before = datetime.now() + timedelta(hours=1)
    coll.trim(before)
    hard, soft = repo.gc()
    assert hard == 0
    assert soft > 0
    coll.refresh()
    for label in labels:
        frm = coll.series(label).frame()
        assert len(frm) == 100009

    # Default timestamp is 10min, so consecutive gc should be a noop
    hard, soft = repo.gc()
    assert hard == 0
    assert soft == 0

    # Set timeout to zero and sleep a bit, to force deletions
    sleep(0.1)
    _timeout = settings.timeout
    settings.timeout = 0
    hard, soft = repo.gc()
    assert hard > 0
    assert soft == 0
    settings.timeout = _timeout

    # Read back data
    coll = repo / "a_collection"
    assert coll.ls() == labels


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

    # Rename to an existing label raise an exception
    assert repo.ls() == ["B", "C", "D"]
    with pytest.raises(ValueError):
        repo.rename("B", "C")

    # Rename an non existing collection too
    assert repo.ls() == ["B", "C", "D"]
    with pytest.raises(ValueError):
        repo.rename("Z", "E")

    # Rename to a longer label (test for issue #2)
    repo.rename("B", "BB")
    assert repo.ls() == ["BB", "C", "D"]

    # Re-create old label
    clc = repo.create_collection(SCHEMA, "A")
    assert clc.ls() == []


def test_import_export(repo):
    clct = repo.create_collection(SCHEMA, "test_coll")
    series = clct / "test_series"
    series.write(
        {
            "timestamp": [1, 2, 3],
            "value": [1, 2, 3],
        }
    )

    tmp_pod = MemPOD(".")
    repo.export_collections(tmp_pod)

    repo_bis = Repo("memory://")
    repo_bis.import_collections(tmp_pod)
    frm = repo.collection("test_coll").series("test_series").frame()
    frm_bis = repo_bis.collection("test_coll").series("test_series").frame()
    assert frm == frm_bis
