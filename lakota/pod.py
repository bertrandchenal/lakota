"""
The `POD` class implement low-level access to different
storage. The `from_uri` method allow to instanciate a POD object based
on a uri. The supported schemes are `file://`, `s3://`, `http://` and
`memory://`. if no scheme is given, the uri is interpreted as a local
path.


``` python-console
>>> from lakota import POD
>>> pod = POD.from_uri('.lakota')
>>> pod.ls()
['00', '01', '02', '03', ... 'fb', 'fc', 'fd', 'fe', 'ff']
```

It is mainly used through the `lakota.Repo` class.
"""
import io
import os
import shutil
import sys
from pathlib import Path, PurePosixPath
from random import choice
from threading import Lock
from time import time
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

try:
    import s3fs
except ImportError:
    s3fs = None

try:
    import requests
except ImportError:
    requests = None

from .utils import logger

__all__ = ["POD", "FilePOD", "MemPOD", "CachePOD"]


class POD:
    _by_token = {}

    def __init__(self):
        self.token = str(uuid4())
        POD._by_token[self.token] = self

    @classmethod
    def from_token(cls, token):
        return cls._by_token[token]

    @classmethod
    def from_uri(cls, uri=None, **fs_kwargs):
        # multi-uri -> CachePOD
        if uri and isinstance(uri, (tuple, list)):
            if len(uri) > 1:
                return CachePOD(
                    local=POD.from_uri(uri[0], **fs_kwargs),
                    remote=POD.from_uri(uri[1:], **fs_kwargs),
                )
            else:
                return POD.from_uri(uri[0], **fs_kwargs)

        # Define protocal and path
        parts = urlsplit(uri or "")
        scheme, path = parts.scheme, parts.path
        kwargs = parse_qs(parts.query)
        if not scheme:
            if not path or path == ":memory:":
                scheme = "memory"
                path = "."
            else:
                scheme = "file"

        # Massage path
        if parts.scheme and path.startswith("/"):
            # urlsplit keep the separator in the path
            path = path[1:]
        if scheme == "file":
            path = Path(path).expanduser()

        # Instatiate pod object
        path = PurePosixPath(path)
        if scheme == "file":
            assert not parts.netloc, "Malformed repo uri, should start with 'file:///'"
            return FilePOD(path)
        elif scheme == "s3":
            if s3fs is None:
                raise ValueError(
                    f'Please install the "s3fs" module in order to access {uri}'
                )
            profile = kwargs.get("profile", [None])[0]
            verify = kwargs.get("verify", [""])[0].lower() != "false"
            return S3POD(path, netloc=parts.netloc, profile=profile, verify=verify)
        elif scheme == "ssh":
            raise NotImplementedError("SSH support not implemented yet")
        elif scheme in ("http", "https"):
            if requests is None:
                raise ImportError(
                    f'Please install the "requests" module in order to access "{uri}"'
                )
            # Build base uri
            base_uri = f"{parts.scheme}://{parts.netloc}/{path}"
            # Extract headers
            headers = {
                k[7:]: v[0] for k, v in kwargs.items() if k.startswith("header-")
            }
            return HttpPOD(base_uri, headers=headers)
        elif scheme == "memory":
            lru_size = int(kwargs.get("lru_size", [0])[0])
            return MemPOD(path, lru_size=lru_size)
        else:
            raise ValueError(f'Protocol "{scheme}" not supported')

    def __truediv__(self, relpath):
        return self.cd(relpath)

    def rm_many(self, pathes, recursive=False):
        for path in pathes:
            self.rm(path, recursive=recursive)

    def walk(self, max_depth=None):
        if max_depth == 0:
            return []

        folders = [("", f, 1) for f in self.ls("")]
        while folders:
            folder = folders.pop()
            root, name, depth = folder
            full_path = str(PurePosixPath(root) / name)
            if self.isdir(full_path):
                if max_depth is not None and depth >= max_depth:
                    continue
                subfolders = [
                    (full_path, c, depth + 1) for c in reversed(self.ls(full_path))
                ]

                folders.extend(subfolders)
            else:
                yield full_path


class FilePOD(POD):

    protocol = "file"

    def __init__(self, path):
        self.path = Path(path)
        super().__init__()

    def cd(self, *others):
        path = self.path.joinpath(*others)
        return FilePOD(path)

    def ls(self, relpath=".", missing_ok=False):
        logger.debug("LIST %s %s", self.path, relpath)
        path = self.path / relpath
        try:
            return list(p.name for p in path.iterdir())
        except FileNotFoundError:
            if missing_ok:
                return []
            raise

    def read(self, relpath, mode="rb"):
        logger.debug("READ %s %s", self.path, relpath)
        path = self.path / relpath
        # XXX make sure path is subpath of self.path
        return path.open(mode).read()

    def write(self, relpath, data, mode="wb"):
        if self.isfile(relpath):
            logger.debug("SKIP-WRITE %s %s", self.path, relpath)
            return
        logger.debug("WRITE %s %s", self.path, relpath)
        path = self.path / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        return path.open(mode).write(data)

    def isdir(self, relpath):
        return self.path.joinpath(relpath).is_dir()

    def isfile(self, relpath):
        return self.path.joinpath(relpath).is_file()

    def rm(self, relpath=".", recursive=False, missing_ok=False):
        logger.debug("REMOVE %s %s", self.path, relpath)
        path = self.path / relpath
        if recursive:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
        elif path.is_dir():
            path.rmdir()
        else:
            try:
                path.unlink()
            except FileNotFoundError:
                if missing_ok:
                    return
                raise

    @property
    def size(self):
        return sum(os.path.getsize(self.path / name) for name in self.walk())


class File:
    """Utility class for MemPOD"""

    def __init__(self, content):
        self.content = content
        self.ts = time()

    def touch(self):
        self.ts = time()


class Folder:
    """Utility class for MemPOD"""

    def __init__(self):
        self.items = {}

    def add(self, key, kind):
        assert kind in (File, Folder)
        current = self.items.get(key)
        if current is not None:
            assert current == kind
        else:
            self.items[key] = kind

    def rm(self, entry):
        self.items.pop(entry, None)

    def ls(self):
        return self.items.keys()


class Store:
    """Utility class for MemPOD"""

    def __init__(self, lru_size=None):
        self.kv = {tuple(): Folder()}
        self._update_lock = Lock()
        self._lru = set()
        self._size = 0
        self.lru_size = None
        if lru_size is not None and lru_size > 0:
            self.lru_size = lru_size

    def get(self, key):
        item = self.kv.get(key)
        if isinstance(item, File):
            item.touch()
        return item

    def set(self, key, value):
        assert isinstance(value, (File, Folder))
        self.kv[key] = value
        if isinstance(value, File):
            self._lru_add(key)
            self._update_size(len(value.content))

    def setdefault(self, key, value):
        assert isinstance(value, (File, Folder))
        return self.kv.setdefault(key, value)

    def delete(self, key):
        try:
            item = self.kv.pop(key)
            if isinstance(item, File):
                self._update_size(-len(item.content))
        except:
            import pdb

            pdb.set_trace()

    def _lru_add(self, path):
        if self.lru_size is None:
            return
        while len(self._lru) >= 20:
            # remove random item
            c = choice(list(self._lru))
            self._lru.discard(c)
        self._lru.add(path)

    def _get_ts(self, k):
        item = self.kv.get(k)
        if item is None:
            return sys.maxsize
        return item.ts

    def _lru_pop(self):
        lru = sorted(self._lru, key=lambda k: self._get_ts(k))
        path = lru[0]
        self._lru.discard(path)
        return path

    def _update_size(self, value):
        self._size += value
        if value < 0 or self.lru_size is None:
            return
        with self._update_lock:
            while self._size > self.lru_size:
                oldest = self._lru_pop()
                self.delete(oldest)


class MemPOD(POD):

    protocol = "memory"

    def __init__(self, path, store=None, lru_size=None):
        self.path = Path(path)
        self.parts = self.path.parts
        self.store = store or Store(lru_size=lru_size)
        super().__init__()

    def cd(self, *others):
        path = Path(*(self.parts + others))
        return MemPOD(path, store=self.store)

    def isdir(self, relpath):
        relpath = self.split(relpath)
        key = self.parts + relpath
        item = self.store.get(key)
        return isinstance(item, Folder)

    def isfile(self, relpath):
        relpath = self.split(relpath)
        key = self.parts + relpath
        item = self.store.get(key)
        return isinstance(item, File)

    def write(self, relpath, data, mode="rb"):
        if self.isfile(relpath):
            logger.debug("SKIP-WRITE memory://%s %s", self.path, relpath)
            return
        logger.debug("WRITE memory://%s %s", "/".join(self.parts), relpath)

        current_path = tuple()
        relpath = self.split(relpath)
        full_path = self.parts + relpath

        for part in full_path:
            parent = current_path
            current_path = current_path + (part,)
            folder = self.store.setdefault(parent, Folder())
            assert isinstance(folder, Folder)
            if current_path == full_path:
                folder.add(part, File)
                current_file = self.store.get(current_path)
                if current_file is not None:
                    assert isinstance(current_file, File)
                    logger.debug(
                        "SKIP-WRITE memory://%s %s", self.path, "/".join(relpath)
                    )
                else:
                    self.store.set(current_path, File(data))
            else:
                folder.add(part, Folder)
        return len(data)

    def ls(self, relpath="", missing_ok=False):
        path = self.parts + self.split(relpath)
        item = self.store.get(path)
        if item is None:
            if missing_ok:
                return []
            raise FileNotFoundError(f'Path "{path}" not found')
        elif isinstance(item, Folder):
            return list(item.ls())
        else:
            return ["/".join(path)]

    def rm(self, relpath=".", recursive=False, missing_ok=False):
        path = self.parts + self.split(relpath)
        item = self.store.get(path)
        if isinstance(item, File):
            item = self.store.delete(path)

        elif isinstance(item, Folder):
            if not recursive:
                raise FileNotFoundError(f"{relpath} is not empty")

            # Delete and recurse
            self.store.delete(path)
            for child in list(item.items):
                self.rm(relpath + "/" + child, recursive=True)

        elif not missing_ok:
            raise FileNotFoundError(f"{relpath} not found")

        # Update folder info
        parent_path = path[:-1]
        parent_folder = self.store.get(parent_path)
        if parent_folder:
            # Note: Parent my not exists in the middle of a
            # recursive deletion
            parent_folder.rm(path[-1])

    def read(self, relpath, mode="rb"):
        logger.debug("READ memory://%s %s", "/".join(self.parts), relpath)
        path = self.parts + self.split(relpath)
        item = self.store.get(path)
        if not isinstance(item, File):
            raise FileNotFoundError(f'Path "{path}" not found')
        return item.content

    @classmethod
    def split(cls, path):
        if not path:
            return tuple()
        if isinstance(path, tuple):
            return path
        if isinstance(path, PurePosixPath):
            return path.parts
        return tuple(p for p in path.split("/") if p != ".")


class CachePOD(POD):
    def __init__(self, local, remote):
        self.local = local
        self.remote = remote
        self.protocol = f"{local.protocol}+{remote.protocol}"
        super().__init__()

    @property
    def path(self):
        return self.local.path

    def cd(self, *others):
        local = self.local.cd(*others)
        remote = self.remote.cd(*others)
        return CachePOD(local, remote)

    def ls(self, relpath=".", missing_ok=False):
        return self.remote.ls(relpath, missing_ok=missing_ok)

    def read(self, relpath, mode="rb"):
        try:
            return self.local.read(relpath, mode=mode)
        except FileNotFoundError:
            pass

        data = self.remote.read(relpath, mode=mode)
        self.local.write(relpath, data)
        return data

    def write(self, relpath, data, mode="wb"):
        self.local.write(relpath, data, mode=mode)
        return self.remote.write(relpath, data, mode=mode)

    def isdir(self, relpath):
        return self.remote.isdir(relpath)

    def isfile(self, relpath):
        return self.remote.isfile(relpath)

    def rm(self, relpath, recursive=False, missing_ok=False):
        self.remote.rm(relpath, recursive=recursive, missing_ok=missing_ok)
        try:
            self.local.rm(relpath, recursive=recursive, missing_ok=missing_ok)
        except FileNotFoundError:
            pass


class SSHPOD(POD):

    protocol = "ssh"

    def __init__(self, client, path):
        self.client = client
        super().__init__()

    @classmethod
    def from_uri(cls, uri):
        user, tail = uri.split("@")
        host, path = tail.split("/", 1)

        # path = Path(path)

        key = os.environ["SSH_KEY"]
        file_obj = io.StringIO(key)

        k = paramiko.RSAKey(file_obj=file_obj)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(hostname=host, username="username", pkey=k)

        return SSHPOD(client, path)

    def cd(self, *others):
        path = self.path.joinpath(*others)
        return SSHPOD(self.client, path)

    def ls(self, relpath=".", missing_ok=False):
        logger.debug("LIST %s %s", self.path, relpath)
        path = self.path / relpath
        try:
            return self.client.listdir(path)
        except FileNotFoundError:
            if missing_ok:
                return []
            raise

    def read(self, relpath, mode="rb"):
        logger.debug("READ %s %s", self.path, relpath)
        path = self.path / relpath
        return self.client.open(path, mode).read()


# Tigger imports if related dependencies are present
if requests is not None:
    from .http_pod import HttpPOD
if s3fs is not None:
    from .s3_pod import S3POD
