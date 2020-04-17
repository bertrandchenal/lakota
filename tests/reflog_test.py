from zarr import MemoryStore


from baltic import RefLog
from baltic.utils import digest



def test_create_refs():
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
