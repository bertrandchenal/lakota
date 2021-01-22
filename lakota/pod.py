import io
import os
import shutil
from pathlib import Path, PurePosixPath
from urllib.parse import parse_qs, urlsplit
from uuid import uuid4

import s3fs
from requests import Session

from .utils import logger

__all__ = ["POD"]


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
        if isinstance(uri, (tuple, list)):
            if len(uri) > 1:
                return CachePOD(
                    local=POD.from_uri(uri[0], **fs_kwargs),
                    remote=POD.from_uri(uri[1:], **fs_kwargs),
                )
            else:
                return POD.from_uri(uri[0], **fs_kwargs)
        elif uri and "+" in uri:
            return POD.from_uri(uri.split("+"), **fs_kwargs)

        # Define protocal and path
        parts = urlsplit(uri)
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
            profile = kwargs.get("profile", [None])[0]
            return S3POD(path, netloc=parts.netloc, profile=profile)
        elif scheme == "ssh":
            raise NotImplementedError("SSH support not implemented yet")
        elif scheme == "http":
            base_uri = f"{parts.scheme}://{parts.netloc}/"
            return HttpPOD(base_uri, path)
        elif scheme == "memory":
            return MemPOD(path)
        else:
            raise ValueError(f'Protocol "{scheme}" not supported')

    def __truediv__(self, relpath):
        return self.cd(relpath)

    def clear(self, *skip):
        for key in self.ls():
            if skip and key in skip:
                continue
            self.rm(key, recursive=True)

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


class MemPOD(POD):

    protocol = "memory"

    def __init__(self, path=".", parent=None):
        self.path = PurePosixPath(path)
        self.parent = parent
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
                pod = MemPOD(path, parent=parent)
                parent.store[frag] = pod
            else:
                return None
        return pod

    def find_parent_pod(self, relpath, auto_mkdir=False):
        fragments = self.split(relpath)
        pod = self._find_pod(fragments[:-1], auto_mkdir)
        return pod, fragments[-1]

    def ls(self, relpath=".", missing_ok=False):
        logger.debug("LIST memory://%s %s", self.path, relpath)
        pod, leaf = self.find_parent_pod(relpath)
        # Handle pathological cases
        if not pod:
            if missing_ok:
                return []
            raise FileNotFoundError(f"{relpath} not found")
        elif leaf not in pod.store:
            if leaf == ".":
                return list(self.store.keys())
            elif missing_ok:
                return [leaf]
            raise FileNotFoundError(f"{relpath} not found")
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
            raise FileNotFoundError(f"{leaf} not found in {pod.path}")
        if isinstance(pod.store[leaf], POD):
            raise FileNotFoundError(f"{leaf} is a directory in {pod.path}")
        return pod.store[leaf]

    def write(self, relpath, data, mode="wb"):
        pod, leaf = self.find_parent_pod(relpath, auto_mkdir=True)
        if not pod:
            raise FileNotFoundError(f"{relpath} not found")
        if leaf in pod.store:
            logger.debug("SKIP-WRITE memory://%s %s", self.path, relpath)
            return
        logger.debug("WRITE memory://%s %s", self.path, relpath)
        pod.store[leaf] = data
        return len(data)

    def isdir(self, relpath):
        pod, leaf = self.find_parent_pod(relpath)
        if pod is None:
            return False
        return isinstance(pod.store[leaf], POD)

    def isfile(self, relpath):
        pod, leaf = self.find_parent_pod(relpath)
        if pod is None:
            return False
        return leaf in pod.store and not isinstance(pod.store[leaf], POD)

    def rm(self, relpath, recursive=False, missing_ok=False):
        logger.debug("REMOVE memory://%s %s", self.path, relpath)
        if relpath == ".":
            if not self.parent:
                raise FileNotFoundError('Not parent for "."')
            pod = self.parent
            leaf = self.split(self.path)[-1]
        else:
            pod, leaf = self.find_parent_pod(relpath)

        if not pod:
            if missing_ok:
                return
            raise FileNotFoundError(f"{relpath} not found")
        if leaf not in pod.store:
            if missing_ok:
                return
            else:
                msg = f'File "{leaf}" not found in "{pod.path}"'
                raise FileNotFoundError(msg)

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

    def __init__(self, path, netloc=None, profile=None, fs=None):
        # TODO document use of param: endpoint_url='http://127.0.0.1:5300'
        self.path = path
        if fs:
            self.fs = fs
        else:
            client_kwargs = {}
            if netloc:
                # TODO support for https
                client_kwargs["endpoint_url"] = f"http://{netloc}"
            self.fs = s3fs.S3FileSystem(
                anon=False, client_kwargs=client_kwargs, profile=profile
            )

        super().__init__()

    def cd(self, *others):
        path = self.path.joinpath(*others)
        return S3POD(path, fs=self.fs)

    def ls(self, relpath=".", missing_ok=False):
        logger.debug("LIST s3://%s %s", self.path, relpath)
        path = self.path / relpath
        try:
            return [Path(p).name for p in self.fs.ls(path)]
        except FileNotFoundError:
            if missing_ok:
                return []
            raise

    def read(self, relpath, mode="rb"):
        logger.debug("READ s3://%s %s", self.path, relpath)
        path = str(self.path / relpath)
        return self.fs.open(path, mode).read()

    def write(self, relpath, data, mode="wb"):
        if self.isfile(relpath):
            logger.debug("SKIP-WRITE s3://%s %s", self.path, relpath)
            return
        logger.debug("WRITE s3://%s %s", self.path, relpath)
        path = str(self.path / relpath)
        return self.fs.open(path, mode).write(data)

    def isdir(self, relpath):
        return self.fs.isdir(self.path / relpath)

    def isfile(self, relpath):
        return self.fs.isfile(self.path / relpath)

    def rm(self, relpath=".", recursive=False, missing_ok=False):
        logger.debug("REMOVE s3://%s %s", self.path, relpath)
        path = str(self.path / relpath)
        try:
            return self.fs.rm(path, recursive=recursive)
        except FileNotFoundError:
            if missing_ok:
                return
            raise


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


class HttpPOD(POD):

    protocol = "http"

    def __init__(self, base_uri, path=None, session=None):
        self.base_uri = base_uri
        self.path = path
        self.session = session or Session()
        super().__init__()

    def cd(self, *others):
        path = self.path.joinpath(*others)
        return HttpPOD(self.base_uri, path, session=self.session)

    def ls(self, relpath=".", missing_ok=False):
        logger.debug("LIST %s://%s %s", self.protocol, self.path, relpath)
        path = self.path / relpath
        resp = self.session.get(self.base_uri + "ls/" + str(path))

        if resp.status_code == 404:
            if missing_ok:
                return []
            raise FileNotFoundError(f"{relpath} not found")
        else:
            resp.raise_for_status()

        return resp.text.splitlines()

    def read(self, relpath, mode="rb"):
        logger.debug("READ %s://%s %s", self.protocol, self.path, relpath)
        path = self.path / relpath
        resp = self.session.get(self.base_uri + "read/" + str(path))

        if resp.status_code == 404:
            raise FileNotFoundError(f"{relpath} not found")
        else:
            resp.raise_for_status()
        return resp.content

    def write(self, relpath, data, mode="wb"):
        logger.debug("WRITE %s://%s %s", self.protocol, self.path, relpath)
        path = str(self.path / relpath)
        resp = self.session.post(self.base_uri + "write/" + str(path), data=data)
        resp.raise_for_status()
        return int(resp.content) if resp.content else None

    def rm(self, relpath=".", recursive=False, missing_ok=False):
        logger.debug("REMOVE %s://%s %s", self.protocol, self.path, relpath)
        path = str(self.path / relpath)
        params = {
            "recursive": "true" if recursive else "",
            "missing_ok": "true" if missing_ok else "",
        }
        resp = self.session.post(self.base_uri + "rm/" + path, params=params)
        resp.raise_for_status()

    def walk(self, max_depth=None):
        if max_depth == 0:
            return []
        params = {}
        if max_depth is not None:
            params["max_depth"] = str(max_depth)

        resp = self.session.get(self.base_uri + "walk/" + str(self.path), params=params)
        resp.raise_for_status()
        return resp.text.splitlines()
