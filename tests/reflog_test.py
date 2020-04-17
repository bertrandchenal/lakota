from time import sleep
from random import random
from concurrent.futures import ThreadPoolExecutor

from zarr import MemoryStore


from baltic import RefLog
from baltic.utils import digest



def test_commit():
    # Create 3 changeset in series
    datum = b'ham spam foo bar baz'.split()
    store = MemoryStore()

    reflog = RefLog(store)
    for data in datum:
        key = digest(data)
        timestamp = 1234
        author = 'Doe'
        info = f'{key} {timestamp} {author}'.encode()
        reflog.commit(digest(info), info)

    res = list(store)
    assert len(res) == len(datum)

    for name, expected in zip(reflog.walk(), datum):
        data = store.get(name)
        assert data.decode().startswith(digest(expected))

def test_concurrent_commit():
    datum = b'ham spam foo bar baz'.split()
    store = MemoryStore()
    reflogs = [RefLog(store) for _ in range(len(datum))]

    contents = []
    for data in datum:
        key = digest(data)
        timestamp = 1234
        author = 'Doe'
        info = f'{key} {timestamp} {author}'.encode()
        contents.append(info)


    with ThreadPoolExecutor() as executor:
        futs = []
        for reflog, info in zip(reflogs, contents):
            f = executor.submit(reflog.commit, info, _jitter=True)
            futs.append(f)
        executor.shutdown()

    for f in futs:
        assert not f.exception()

    res = list(store)
    assert len(res) == len(datum)

    # for name, expected in zip(reflog.walk(), datum):
    #     data = store.get(name)
    #     assert data.decode().startswith(digest(expected))
