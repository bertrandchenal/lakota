import pytest

from baltic import POD


def test_cd(pod):
    pod2 = pod / "ham"
    assert pod2.path.name == "ham"


def test_empty_ls(pod):
    pod = POD.from_uri("file://i-do-not-exists")
    with pytest.raises(FileNotFoundError):
        pod.ls()

    assert pod.ls(raise_on_missing=False) == []


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


def test_write_clear(pod):
    data = bytes.fromhex("DEADBEEF")

    pod.write("key", data)
    pod.write("ham/key", data)
    pod.write("ham/spam/key", data)

    assert len(pod.ls()) == 2
    assert len(pod.ls("ham")) == 2
    assert len(pod.ls("ham/spam")) == 1
    pod.clear()
    assert pod.ls() == []
