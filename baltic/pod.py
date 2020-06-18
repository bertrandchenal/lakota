from pathlib import Path, PurePosixPath
import shutil


class POD:

    @classmethod
    def from_uri(cls, uri=None, **fs_kwargs):
        if not uri:
            protocol = 'file'
            path = '.'
        else:
            protocol, path = uri.split('://', 1)
        path = PurePosixPath(path)
        fs_kwargs.setdefault('auto_mkdir', True)
        if protocol == 'file':
            return FilePOD(path)
        elif protocol == 'memory':
            return MemPOD(path)
        else:
            raise ValueError(f'Protocol "{protocol}" not supported')

    def __truediv__(self, relpath):
        return self.cd(relpath)


class FilePOD(POD):

    def __init__(self, path):
        self.path = Path(path)

    def cd(self, relpath):
        path = self.path / relpath
        return FilePOD(path)

    def ls(self, relpath='.', raise_on_missing=True):
        path = self.path / relpath
        try:
            return list(p.name for p in path.iterdir())
        except FileNotFoundError:
            if raise_on_missing:
                raise
            return []

    def read(self, relpath, mode='rb'):
        path = self.path / relpath
        return path.open(mode).read()

    def write(self, relpath, data, mode='wb'):
        # XXX skip write if file exist ? mode=cb ?
        path = self.path / relpath
        path.parent.mkdir(parents=True, exist_ok=True)
        return path.open(mode).write(data)

    def rm(self, relpath='.', recursive=False):
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

    def clear(self):
        for key in self.ls():
            self.rm(key, recursive=True)


class MemPOD(POD):

    def __init__(self, path):
        self.path = PurePosixPath(path)
        # store keys are path, values are either bytes (aka a file)
        # either another dict (aka a directory)
        self.store = {}

    def cd(self, relpath):
        pod = self.find_pod(relpath)
        return pod

    @classmethod
    def split(cls, path):
        return PurePosixPath(path).as_posix().split('/')

    def find_pod(self, relpath, auto_mkdir=True):
        fragments = self.split(relpath)
        return self._find_pod(fragments, auto_mkdir)

    def _find_pod(self, fragments, auto_mkdir=False):
        path = self.path
        pod = self
        for frag in fragments:
            if frag == '.':
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

    def ls(self, relpath='.', raise_on_missing=True):
        pod,leaf = self.find_parent_pod(relpath)
        # Handle pathological cases
        if not pod:
            if raise_on_missing:
                raise FileNotFoundError(f'{relpath} not found')
            return []
        elif leaf not in pod.store:
            if leaf == '.':
                return list(self.store.keys())
            elif raise_on_missing:
                raise FileNotFoundError(f'{relpath} not found')
            return [leaf]
        # "happy" scenario
        if isinstance(pod.store[leaf], POD):
            return list(pod.store[leaf].store.keys())
        else:
            return [leaf]

    def read(self, relpath, mode='rb'):
        pod, leaf = self.find_parent_pod(relpath)
        if not pod:
            raise FileNotFoundError(f'{relpath} not found')
        if leaf not in pod.store:
            raise FileNotFoundError('{leaf} not found in {pod.path}')
        return pod.store[leaf]

    def write(self, relpath, data, mode='wb'):
        pod, leaf = self.find_parent_pod(relpath, auto_mkdir=True)
        if not pod:
            raise FileNotFoundError(f'{relpath} not found')
        pod.store[leaf] = data

    def rm(self, relpath, recursive=False):
        pod, leaf = self.find_parent_pod(relpath)
        if not pod:
            raise FileNotFoundError(f'{relpath} not found')
        if recursive :
            del pod.store[leaf]
        elif isinstance(pod.store[leaf], MemPOD):
            if not pod.store[leaf].store.empty():
                raise FileNotFoundError(f'{relpath} is not empty')
            del pod.store[leaf]
        else:
            del pod.store[leaf]

    def clear(self):
        self.store = {}
