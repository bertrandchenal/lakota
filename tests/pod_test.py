import pytest

from lakota.pod import S3POD, CachePOD, FilePOD, MemPOD


def test_cd(pod):
    pod2 = pod / "ham"
    assert pod2.path.name == "ham"


def test_simple_ls(pod):
    if not isinstance(pod, (S3POD, CachePOD)):
        # Note: s3fs does not complain when listing a non-existing
        # path (only if the bucket is missing)
        with pytest.raises(FileNotFoundError):
            pod.ls("A")
    assert pod.ls("B", missing_ok=True) == []

    data = b"DEADBEEF"
    # Add file in a folder
    pod.write("A/a", data)
    assert pod.ls() == ["A"]
    assert pod.ls("A") == ["a"]

    # Add top-level file
    pod.write("B", data)
    assert sorted(pod.ls()) == ["A", "B"]

    # Add file in sub-pod
    sub_pod = pod.cd("C/D")
    sub_pod.write("d", data)
    assert sorted(pod.ls("C/D")) == ["d"]


def test_read_write(pod):
    data = bytes.fromhex("DEADBEEF")
    pod.write("key", data)
    assert pod.ls() == ["key"]
    res = pod.read("key")
    assert res == data


def test_multi_write(pod):
    data = bytes.fromhex("DEADBEEF")
    # First write
    res = pod.write("key", data)
    assert res == len(data)
    # second one
    res = pod.write("key", data)
    assert res is None


def test_write_delete(pod):
    data = bytes.fromhex("DEADBEEF")

    pod.write("key", data)
    pod.rm("key")
    assert pod.ls() == []


def test_write_delete_recursive(pod):
    data = bytes.fromhex("DEADBEEF")
    top_pod = pod.cd("top_dir")

    top_pod.write("sub_dir/key", data)
    if isinstance(pod, MemPOD):
        with pytest.raises(FileNotFoundError):
            top_pod.rm(".")
    elif isinstance(pod, FilePOD):
        with pytest.raises(OSError):
            top_pod.rm(".")
    # not test for S3, it seems that recurssion is implied

    top_pod.rm(".", recursive=True)
    assert pod.ls() == []


def test_write_clear(pod):
    assert pod.ls() == []
    data = bytes.fromhex("DEADBEEF")

    pod.write("key", data)
    pod.write("ham/key", data)
    pod.write("ham/spam/key", data)

    assert len(pod.ls()) == 2
    assert len(pod.ls("ham")) == 2
    assert len(pod.ls("ham/spam")) == 1
    pod.clear()
    assert (
        pod.ls(
            missing_ok=True  # moto_server delete the bucket when all keys are removed
        )
        == []
    )


def test_walk(pod):
    data = b""
    pod.write("ham/spam/foo", data)
    pod.write("bar/baz", data)
    pod.write("qux", data)

    assert sorted(pod.walk()) == ["bar/baz", "ham/spam/foo", "qux"]
    assert sorted(pod.walk(max_depth=10)) == ["bar/baz", "ham/spam/foo", "qux"]
    assert sorted(pod.walk(max_depth=3)) == ["bar/baz", "ham/spam/foo", "qux"]
    assert sorted(pod.walk(max_depth=2)) == ["bar/baz", "qux"]
    assert sorted(pod.walk(max_depth=1)) == ["qux"]
    assert sorted(pod.walk(max_depth=0)) == []
