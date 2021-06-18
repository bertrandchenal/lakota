import pytest

from lakota.pod import POD, S3POD, CachePOD, FilePOD, MemPOD

deadbeef = bytes.fromhex("DEADBEEF")


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
    pod.write("key", data)
    assert pod.ls() == ["key"]
    res = pod.read("key")
    assert res == deadbeef


def test_multi_write(pod):
    # First write
    res = pod.write("key", deadbeef)
    assert res == len(deadbeef)
    # second one
    res = pod.write("key", deadbeef)
    assert res is None


def test_write_delete(pod):
    pod.write("key", deadbeef)
    pod.rm("key")
    assert pod.ls() == []


def test_write_delete_recursive(pod):
    top_pod = pod.cd("top_dir")

    top_pod.write("sub_dir/key", deadbeef)
    if isinstance(pod, MemPOD):
        with pytest.raises(FileNotFoundError):
            top_pod.rm(".")
    elif isinstance(pod, FilePOD):
        with pytest.raises(OSError):
            top_pod.rm(".")
    # not test for S3, it seems that recurssion is implied

    top_pod.rm(".", recursive=True)
    assert pod.ls() == []


def test_write_rm_many(pod):
    assert pod.ls() == []

    pod.write("key", deadbeef)
    pod.write("ham/key", deadbeef)
    pod.write("ham/spam/key", deadbeef)

    assert len(pod.ls()) == 2
    assert len(pod.ls("ham")) == 2
    assert len(pod.ls("ham/spam")) == 1
    pod.rm_many(["ham/spam/key"])
    assert pod.ls("ham/spam") == []

    pod.rm_many(["ham", "key"], recursive=True)
    res = pod.ls(
        missing_ok=True,  # moto_server delete the bucket when all keys are removed
    )
    assert res == []


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

    sub_pod = pod.cd("bar")
    assert sorted(sub_pod.walk()) == ["baz"]
    assert sorted(sub_pod.walk(max_depth=10)) == ["baz"]
    assert sorted(sub_pod.walk(max_depth=3)) == ["baz"]
    assert sorted(sub_pod.walk(max_depth=2)) == ["baz"]


def test_s3_with_secret():
    pod = POD.from_uri("s3:///some/bucket?key=key&secret=secret&token=token")
    assert isinstance(pod, S3POD)
    assert str(pod.path) == "some/bucket"
    assert pod.fs.key == "key"


def test_mempod_lru():
    pod = MemPOD("/", lru_size=100 * len(deadbeef))

    # Fill the pod until the selected limit
    for i in range(1, 101):
        pod.write(str(i), deadbeef)
        # Pod by default will contain an empty root and the selected
        # root ("/"). hence the `+2`
        assert len(pod.store.kv) == i + 2

    # Any extra write will trigger a discard of an "older" file
    for i in range(101, 111):
        pod.write(str(i), deadbeef)
    assert len(pod.store.kv) == 102
    assert pod.store._size

    # Detect discarded files, makes sure newest files are still there
    cnt = 0
    for i in range(1, 110):
        try:
            pod.read(str(i))
        except FileNotFoundError:
            assert i < 101
            cnt += 1
    assert cnt == 10
