import pytest

from baltic import POD

TODO add fixture that always return new pod (even form type memory)

def test_cd():
    pod = POD.from_uri()
    pod2 = pod / 'ham'
    assert str(pod2.path) == '/ham'

def test_uri():
    pod = POD.from_uri('memory:///')
    assert str(pod.path) == '/'

    pod = POD.from_uri('memory://foo')
    assert str(pod.path) == 'foo'


def test_ls():
    pod = POD.from_uri('memory://')
    assert pod.ls() == []
    assert pod.ls('spam') == []

    pod = POD.from_uri('file://i-do-not-exists')
    with pytest.raises(FileNotFoundError):
        pod.ls()

    assert pod.ls(if_missing='pass') == []

def test_read_write():
    pod = POD.from_uri()
    data = bytes.fromhex('DEADBEEF')

    pod.write('key', data)
    assert pod.ls() == ['/key']
    res = pod.read('key')
    assert res == data

def test_write_delete():
    pod = POD.from_uri()
    data = bytes.fromhex('DEADBEEF')

    pod.write('key', data)
    pod.rm('key')
    assert pod.ls() == []
