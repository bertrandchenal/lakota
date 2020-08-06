import shutil
from pathlib import Path, PurePosixPath
from uuid import uuid4

import s3fs

from .utils import logger


class POD:
    _by_token = {}

    def __init__(self):
        self.token = str(uuid4())
        POD._by_token[self.token] = self

    @classmethod
    def from_token(cls, token):
        return cls._by_token[token]

    @classmethod
    def from_uri(cls, uri=None, lazy=False, **fs_kwargs):
        # multi-uri -> CachePOD
        if isinstance(uri, (tuple, list)):
            if len(uri) > 1:
                return CachePOD(
                    local=POD.from_uri(uri[0], **fs_kwargs),
                    remote=POD.from_uri(uri[1:], **fs_kwargs),
                    lazy=lazy,
                )
            else:
                return POD.from_uri(uri[0], **fs_kwargs)
        elif uri and "+" in uri:
            return POD.from_uri(uri.split("+"), lazy=lazy, **fs_kwargs)

        # Define protocal and path
        if not uri:
            protocol = "memory"
            path = "."
        elif not "://" in uri:
            protocol = "file"
            path = uri
        else:
            protocol, path = uri.split("://", 1)

        # Instatiate pod object
        path = PurePosixPath(path)
        fs_kwargs.setdefault("auto_mkdir", True)
        if protocol == "file":
            return FilePOD(path)
        elif protocol == "s3":
            return S3POD(path)
        elif protocol == "memory":
            return MemPOD(path)
        else:
            raise ValueError(f'Protocol "{protocol}" not supported')

    def __truediv__(self, relpath):
        return self.cd(relpath)

    def clear(self, *skip):
        for key in self.ls():
            if skip and key in skip:
                continue
            self.rm(key, recursive=True)

    def walk(self, root=""):
        folders = [(root, f) for f in self.ls(root)]
        while folders:
            folder = folders.pop()
            root, name = folder
            full_path = str(PurePosixPath(root) / name)
            if self.isdir(full_path):
                subfolders = [(full_path, c) for c in reversed(self.ls(full_path))]
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

    def ls(self, relpath=".", raise_on_missing=True):
        logger.debug("LIST %s %s", self.path, relpath)
        path = self.path / relpath
        try:
            return list(p.name for p in path.iterdir())
        except FileNotFoundError:
            if raise_on_missing:
                raise
            return []

    def read(self, relpath, mode="rb"):
        logger.debug("READ %s %s", self.path, relpath)
        path = self.path / relpath
        return path.open(mode).read()

    def write(self, relpath, data, mode="wb"):
        logger.debug("WRITE %s %s", self.path, relpath)
        if relpath in self.ls(raise_on_missing=False):
            return
        path = self.path / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        return path.open(mode).write(data)

    def isdir(self, relpath):
        return self.path.joinpath(relpath).is_dir()

    def rm(self, relpath=".", recursive=False):
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
            path.unlink()


class MemPOD(POD):

    protocol = "memory"

    def __init__(self, path):
        self.path = PurePosixPath(path)
        # store keys are path, values are either bytes (aka a file)
        # either another dict (aka a directory)
        self.store = {}
        super().__init__()

    def cd(self, *others):
        pod = self
        for o in others:
            pod = pod.find_pod(o)
        return pod

    @classmethod
    def split(cls, path):
        return PurePosixPath(path).as_posix().split("/")

    def find_pod(self, relpath, auto_mkdir=True):
        fragments = self.split(relpath)
        return self._find_pod(fragments, auto_mkdir)

    def _find_pod(self, fragments, auto_mkdir=False):
        path = self.path
        pod = self
        for frag in fragments:
            if frag == ".":
                continue
            path = path / frag
            if frag in pod.store:
                pod = pod.store[frag]
            elif auto_mkdir:
                parent = pod
                pod = MemPOD(path)
                parent.store[frag] = pod
            else:
                return None
        return pod

    def find_parent_pod(self, relpath, auto_mkdir=False):
        fragments = self.split(relpath)
        pod = self._find_pod(fragments[:-1], auto_mkdir)
        return pod, fragments[-1]

    def ls(self, relpath=".", raise_on_missing=True):
        logger.debug("LIST memory://%s %s", self.path, relpath)
        pod, leaf = self.find_parent_pod(relpath)
        # Handle pathological cases
        if not pod:
            if raise_on_missing:
                raise FileNotFoundError(f"{relpath} not found")
            return []
        elif leaf not in pod.store:
            if leaf == ".":
                return list(self.store.keys())
            elif raise_on_missing:
                raise FileNotFoundError(f"{relpath} not found")
            return [leaf]
        # "happy" scenario
        if pod.isdir(leaf):
            return list(pod.store[leaf].store.keys())
        else:
            return [leaf]

    def read(self, relpath, mode="rb"):
        logger.debug("READ memory://%s %s", self.path, relpath)
        pod, leaf = self.find_parent_pod(relpath)
        if not pod:
            raise FileNotFoundError(f"{relpath} not found")
        if leaf not in pod.store:
            raise FileNotFoundError("{leaf} not found in {pod.path}")
        return pod.store[leaf]

    def write(self, relpath, data, mode="wb"):
        logger.debug("WRITE memory://%s %s", self.path, relpath)
        if relpath in self.ls(raise_on_missing=False):
            return
        pod, leaf = self.find_parent_pod(relpath, auto_mkdir=True)
        if not pod:
            raise FileNotFoundError(f"{relpath} not found")
        pod.store[leaf] = data
        return len(data)

    def isdir(self, relpath):
        pod, leaf = self.find_parent_pod(relpath)
        return isinstance(pod.store[leaf], POD)

    def rm(self, relpath, recursive=False):
        logger.debug("REMOVE memory://%s %s", self.path, relpath)
        pod, leaf = self.find_parent_pod(relpath)
        if not pod:
            raise FileNotFoundError(f"{relpath} not found")
        if recursive:
            del pod.store[leaf]
        elif pod.isdir(leaf):
            if pod.store[leaf].store:
                raise FileNotFoundError(f"{relpath} is not empty")
            del pod.store[leaf]
        else:
            del pod.store[leaf]


class S3POD(POD):

    protocol = "s3"

    def __init__(self, path, fs=None):
        self.path = path
        self.fs = fs or s3fs.S3FileSystem(
            anon=False,
            # client_kwargs={'endpoint_url': 'http://127.0.0.1:9000'},
            # key='minioadmin',
            # secret='minioadmin'
        )
        super().__init__()

    def cd(self, *others):
        path = self.path.joinpath(*others)
        return S3POD(path, fs=self.fs)

    def ls(self, relpath=".", raise_on_missing=True):
        logger.debug("LIST s3://%s %s", self.path, relpath)
        path = self.path / relpath
        try:
            return [Path(p).name for p in self.fs.ls(path)]
        except FileNotFoundError:
            if raise_on_missing:
                raise
            return []

    def read(self, relpath, mode="rb"):
        logger.debug("READ s3://%s %s", self.path, relpath)
        path = str(self.path / relpath)
        return self.fs.open(path, mode).read()

    def write(self, relpath, data, mode="wb"):
        logger.debug("WRITE s3://%s %s", self.path, relpath)
        if relpath in self.ls(raise_on_missing=False):
            return
        path = str(self.path / relpath)
        return self.fs.open(path, mode).write(data)

    def isdir(self, relpath):
        return self.fs.isdir(self.path / relpath)

    def rm(self, relpath=".", recursive=False):
        logger.debug("REMOVE s3://%s %s", self.path, relpath)
        path = str(self.path / relpath)
        return self.fs.rm(path, recursive=recursive)


class CachePOD(POD):
    def __init__(self, local, remote, lazy=False):
        self.local = local
        self.remote = remote
        self.protocol = f"{local.protocol}+{remote.protocol}"
        self.lazy = lazy
        super().__init__()

    @property
    def path(self):
        return self.local.path

    def cd(self, *others):
        local = self.local.cd(*others)
        remote = self.remote.cd(*others)
        return CachePOD(local, remote, lazy=self.lazy)

    def ls(self, relpath=".", raise_on_missing=True):
        if self.lazy:
            return self.local.ls(relpath, raise_on_missing=raise_on_missing)
        return self.remote.ls(relpath, raise_on_missing=raise_on_missing)

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

    def rm(self, relpath, recursive=False):
        self.remote.rm(relpath, recursive=recursive)
        try:
            self.local.rm(relpath, recursive=recursive)
        except FileNotFoundError:
            pass
