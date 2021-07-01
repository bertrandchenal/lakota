"""
The `Repo` class manage the organisation of a storage location. It
provides creation and deletion of collections, synchronization with
remote repositories and garbage collection.


## Create repositories

Create a `Repo` instance:
```python
# in-memory
repo = Repo()
repo = Repo("memory://")
# From a local directory
repo = Repo('some/local/path')
repo = Repo('file://some/local/path')
# From an S3 location
repo = Repo('s3:///my_bucket')
# Use a list of uri to enable caching
repo = Repo(['memory://', 's3:///my_bucket'])
repo = Repo(['file:///tmp/local_cache', 's3:///my_bucket'])
```

S3 authentication is handled by
[s3fs](https://s3fs.readthedocs.io/en/latest/#credentials "s3fs
credentials"). So you can either put your credentials in a
configuration files or in environment variables. If it's not possible,
you can still pass them as arguments:

```python
pod = POD.from_uri('s3:///bucket_name', key=key, secret=secret, token=token)
repo = Repo(pod=pod)
```

Similarly, you can use a compatible service through the `endpoint_url` parameter:

```python
pod = POD.from_uri('s3:///bucket_name', endpoint_url='http://127.0.0.1:5300')
repo = Repo(pod=pod)
```

## Access collections

Create one or several collections:
```python
# Define schema
schema = Schema(timestamp='int*', value='float')
# Create one collection
repo.create_collection(schema, 'my_collection')
# Create a few more
labels = ['one', 'or_more', 'labels']
repo.create_collection(schema, *labels)
```

List and instanciate collections
```python
print(list(repo.ls())) # Print collections names
# Instanciate a collection
clct = repo.collection('my_collection')
# like pathlib, the `/` operator can be used
clct = repo / 'my_collection'
```

See `lakota.collection` on how to manipulate collections


## Garbage Collection

After some times, some series can be overwritten, deleted, squashed or
merged. Sooner or later some pieces of data will get dereferenced,
those can be deleted to recover storage space. It is simply done with
the `gc` method, which returns the number of deleted files.

```python
nb_file_deleted = repo.gc()
```
"""

from itertools import chain

from .changelog import zero_hash
from .collection import Collection
from .pod import POD
from .schema import Schema
from .utils import Pool, hashed_path, hexdigest, logger

__all__ = ["Repo"]


class Repo:
    schema = Schema.kv(label="str*", meta="O")

    def __init__(self, uri=None, pod=None):
        """
        `uri` : a string or a list of string representing a storage
        location

        `pod`
        : a `lakota.pod.POD` instance
        """
        pod = pod or POD.from_uri(uri)
        folder, filename = hashed_path(zero_hash)
        self.pod = pod
        path = folder / filename
        self.registry = Collection("registry", self.schema, path, self)

    def ls(self):
        return [item.label for item in self.search()]

    def __iter__(self):
        return self.search()

    def search(self, label=None, namespace="collection"):
        if label:
            start = stop = (label,)
        else:
            start = stop = None
        series = self.registry.series(namespace)
        qr = series[start:stop] @ {"closed": "BOTH"}
        frm = qr.frame()
        for l in frm["label"]:
            yield self.collection(l, frm)

    def __truediv__(self, name):
        return self.collection(name)

    def collection(self, label, from_frm=None, namespace="collection"):
        series = self.registry.series(namespace)
        if from_frm:
            frm = from_frm.slice(*from_frm.index_slice([label], [label], closed="BOTH"))
        else:
            frm = series.frame(start=label, stop=label, closed="BOTH")

        if frm.empty:
            return None
        meta = frm["meta"][-1]
        return self.reify(label, meta)

    def create_collection(
        self, schema, *labels, raise_if_exists=True, namespace="collection"
    ):
        """
        `schema`
        : A `lakota.schema.Schema` instance

        `labels`
        : One or more collection name

        `raise_if_exists`
        : Raise an exception if the label is already present
        """
        assert isinstance(
            schema, Schema
        ), "The schema parameter must be an instance of lakota.Schema"
        meta = []
        schema_dump = schema.dumps()

        # TODO assert collection does not already exists! (and use raise_if_exists)

        series = self.registry.series(namespace)
        for label in labels:
            label = label.strip()
            if len(label) == 0:
                raise ValueError(f"Invalid label: {label}")

            key = label.encode()
            # Use digest to create collection folder (based on mode and label)
            digest = hexdigest(key)
            if namespace != "collection":
                digest = hexdigest(digest.encode(), namespace.encode())
            folder, filename = hashed_path(digest)
            meta.append({"path": str(folder / filename), "schema": schema_dump})

        series.write({"label": labels, "meta": meta})
        res = [self.reify(l, m) for l, m in zip(labels, meta)]
        if len(labels) == 1:
            return res[0]
        return res

    def reify(self, name, meta):
        schema = Schema.loads(meta["schema"])
        return Collection(name, schema, meta["path"], self)

    def archive(self, collection):
        label = collection.label
        archive = self.collection(label, mode="archive")
        if archive:
            return archive
        return self.create_collection(collection.schema, label, mode="archive")

    def delete(self, *labels, namespace="collection"):
        """
        Delete one or more collections

        `*labels`
        : Strings, names of the collection do delete

        """
        to_remove = []
        for l in labels:
            clct = self.collection(l)
            if not clct:
                continue
            to_remove.append(clct.changelog.pod)
        series = self.registry.series(namespace)
        series.delete(*labels)
        for pod in to_remove:
            try:
                pod.rm(".", recursive=True)
            except FileNotFoundError:
                continue

    def refresh(self):
        self.registry.refresh()

    def push(self, remote, *labels, shallow=False):
        """
        Push local revisions (and related segments) to `remote` Repo.
        `remote`
        : A `lakota.repo.Repo` instance

        `labels`
        : The collections to push. If not given, all collections are pushed
        """
        return remote.pull(self, *labels, shallow=shallow)

    def pull(self, remote, *labels, shallow=False):
        """
        Pull revisions from `remote` Repo (and related segments).
        `remote`
        : A `lakota.repo.Repo` instance

        `labels`
        : The collections to pull. If not given, all collections are pulled
        """

        assert isinstance(remote, Repo), "A Repo instance is required"
        # Pull registry
        self.registry.pull(remote.registry, shallow=shallow)
        # Extract frames
        local_cache = {l.label: l for l in self.search()}
        remote_cache = {r.label: r for r in remote.search()}
        if not labels:
            labels = remote_cache.keys()
        for label in labels:
            logger.info("Sync collection: %s", label)
            r_clct = remote_cache[label]
            if not label in local_cache:
                l_clct = self.create_collection(r_clct.schema, label)
            else:
                l_clct = local_cache[label]
                if l_clct.schema != r_clct.schema:
                    msg = (
                        f'Unable to sync collection "{label}",'
                        "incompatible meta-info."
                    )
                    raise ValueError(msg)
            l_clct.pull(r_clct, shallow=shallow)

    def merge(self):
        """
        Merge repository registry. Needed when collections have been created
        or deleted concurrently.
        """
        return self.registry.merge()

    def rename(self, from_label, to_label, namespace="collection"):
        """
        Change the label a collection
        """
        series = self.registry.series(namespace)
        frm = series.frame()
        if to_label in frm["label"]:
            raise ValueError(f'Collection "{to_label}" already exists')

        # replace in label column
        start, stop = frm.start(), frm.stop()
        labels = frm["label"]
        mask = labels == from_label
        labels[mask] = to_label
        frm["label"] = labels

        # Re-order frame
        frm = frm.sorted()
        series.write(
            frm,
            start=min(
                frm.start(), start
            ),  # Make sure we over-write the previous content
            stop=max(frm.stop(), stop),  # same
        )

    def gc(self):
        """
        Loop on all series, collect all used digests, and delete obsolete
        ones.
        """
        # Collect digests across folders
        base_folders = self.pod.ls()
        with Pool() as pool:
            for folder in base_folders:
                pool.submit(self._walk_folder, folder)
        all_dig = set(chain(*pool.results))

        # Collect digest from changelogs. Because revision (in
        # changelog ) are written after the segments (in folders), we
        # are sure to not delete data created concurrently.
        self.refresh()
        active_digests = set(self.registry.digests())
        for namespace in self.registry.ls():
            for clct in self.search(namespace=namespace):
                active_digests.update(clct.digests())

        # Delete files on fs not in changelogs
        to_delete = all_dig - active_digests
        for dig in to_delete:
            path = hashed_path(dig)
            print("RM", path)
            self.pod.rm(path, missing_ok=True)

    def _walk_folder(self, folder):
        digs = []
        pod = self.pod.cd(folder)
        for filename in pod.walk(max_depth=2):
            digs.append(folder + filename.replace("/", ""))

        return digs
