import pytest

from lakota.pod import S3POD, CachePOD, FilePOD, MemPOD

deadbeef = bytes.fromhex("DEADBEEF")


def test_cd(pod):
    pod2 = pod / "ham"
    assert pod2.path.name == "ham"


def test_simple_ls(pod):
    if not isinstance(pod, (S3POD, CachePOD)):
        # Note: s3 does not complain when listing a non-existing
        # path (only if the bucket is missing)
        with pytest.raises(FileNotFoundError):
            pod.ls("A")
    assert pod.ls("B", missing_ok=True) == []

    # Add file in a folder
    pod.write("A/a", deadbeef)
    assert pod.ls() == ["A"]
    assert pod.ls("A") == ["a"]

    # Add top-level file
    pod.write("B", deadbeef)
    assert sorted(pod.ls()) == ["A", "B"]

    # Add file in sub-pod
    sub_pod = pod.cd("C/D")
    sub_pod.write("d", deadbeef)
    assert sorted(pod.ls("C/D")) == ["d"]


def test_read_write(pod):
    pod.write("key", deadbeef)
    assert pod.ls() == ["key"]
    res = pod.read("key")
    assert res == deadbeef

    # By default we don't overwrite
    decode = bytes.fromhex("DEC0DE")
    pod.write("key", decode)
    assert pod.read("key") == deadbeef

    # force=true allows overwrite
    pod.write("key", decode, force=True)
    assert pod.read("key") == decode


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
    top_pod.write("sub_dir2/sub_dir3/key", deadbeef)
    if isinstance(pod, MemPOD):
        with pytest.raises(FileNotFoundError):
            top_pod.rm(".")
    elif isinstance(pod, FilePOD):
        with pytest.raises(OSError):
            top_pod.rm(".")
    # XXX test on exception for S3
    top_pod.rm("sub_dir2", recursive=True)
    assert pod.ls("top_dir") == ["sub_dir"]

    top_pod.rm(".", recursive=True)
    assert pod.ls() == []


def test_write_rm_many(pod):
    assert pod.ls(missing_ok=True) == []

    pod.write("key", deadbeef)
    pod.write("ham/key", deadbeef)
    pod.write("ham/spam/key", deadbeef)

    assert len(pod.ls()) == 2
    assert len(pod.ls("ham")) == 2
    assert len(pod.ls("ham/spam")) == 1
    pod.rm_many(["ham/spam/key"])
    assert pod.ls("ham/spam") == []

    pod.write("foo/key", deadbeef)
    pod.rm_many(["ham", "foo"], recursive=True)
    res = pod.ls(
        missing_ok=True,  # moto_server delete the bucket when all keys are removed
    )
    assert res == ["key"]


def test_mv(pod):
    assert pod.ls(missing_ok=True) == []
    pod.write("key", deadbeef)

    pod.mv("key", "ham/key")
    assert pod.read("ham/key") == deadbeef

    pod.mv("ham/key", "ham/spam/key")
    assert pod.read("ham/spam/key") == deadbeef

    assert pod.ls() == ["ham"]
    assert pod.ls("ham/spam") == ["key"]

    with pytest.raises(FileNotFoundError):
        pod.mv("spam", "ham/spam")

    pod.mv("spam", "ham/spam", missing_ok=True)


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


def test_mempod_lru():
    lru_size = 100 * len(deadbeef)
    pod = MemPOD("/", lru_size=lru_size)

    # Fill the pod until the selected limit
    for i in range(1, 51):
        pod.write(str(i), deadbeef)
        # Pod by default will contain an empty root and the selected
        # path ("/"). hence the `+2`
        assert len(pod.store.front_kv) == i + 2
        assert len(pod.store.back_kv) == 0

    # When the above loop is done, front_kv is full. The very next
    # write will trigger the swap (because the size of the content of
    # front reach lru_size // 2

    for i in range(1, 110):
        pod.write(str(i), deadbeef)
    assert len(pod.store.front_kv) == 9  # aka 109 - 100
    assert pod.store._size <= lru_size

    # Detect discarded files, makes sure newest files are still there
    for i in range(1, 60):
        try:
            pod.read(str(i))
        except FileNotFoundError:
            assert i <= 51
        else:
            assert i > 50

    # Pathological case: write a value bigger than the request lru_size
    large_data = deadbeef * 100
    pod.write("0", large_data)
    # A swap was triggered, the data is already in back:
    assert pod.store.back_kv["/", "0"]
    assert pod.read("0") == large_data
