from os.path import join
from os import listdir
from tempfile import TemporaryDirectory



from baltic import Segment, RefLog, Schema



# class ObjectStore:
#     def __init__(self):
#         self.kv = []

#     def put(self, data):
#         key = sha1(data).hexdigest()
#         self.kv[key]= data

#     def get(self, key):
#         return self.kv[key]

def test_create_refs():
    # Create 3 changeset in series
    datum = b'ham spam foo bar baz'.split()
    store = ObjectStore()

    with TemporaryDirectory() as td:
        reflog = RefLog(td)
        for data in datum:
            key = store.put(data)
            info = f'{key} {timestamp} {author}'
            reflog.save(name, info)

        res = listdir(td)
        assert len(res) > 0

        for name, expected in zip(reflog.walk(), datum):
            data = open(join(td, name), 'rb').read()
            assert data == expected

        # Merge first two items
        reflog.merge(merge_func)
