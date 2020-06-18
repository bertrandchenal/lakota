from pathlib import PurePosixPath

import fsspec


class POD:

    def __init__(self, fs, path):
        assert isinstance(path, PurePosixPath)
        self.fs = fs
        self.path = path

    @classmethod
    def from_uri(cls, uri=None, **fs_kwargs):
        if not uri:
            protocol = 'memory'
            path = '/'
        else:
            protocol, path = uri.split('://', 1)
        path = PurePosixPath(path)
        fs_kwargs.setdefault('auto_mkdir', True)
        from uuid import uuid4
        fs_kwargs.setdefault('token', str(uuid4()))
        fs = fsspec.filesystem(protocol, **fs_kwargs )
        return POD(fs, path)

    def cd(self, relpath):
        path = self.path / relpath
        return POD(self.fs, path)

    def __truediv__(self, relpath):
        return self.cd(relpath)

    def ls(self, relpath='.', if_missing='raise'):
        path = str(self.path / relpath)
        try:
            return self.fs.ls(path)
        except FileNotFoundError:
            if if_missing == 'pass':
                return []
            raise

    def read(self, relpath, mode='rb'):
        path = str(self.path / relpath)
        return self.fs.open(path, mode).read()

    def write(self, relpath, data, mode='wb'):
        # XXX skip write if file exist ? mode=cb ?
        path = str(self.path / relpath)
        return self.fs.open(path, mode).write(data)

    def rm(self, relpath='.', recursive=False):
        path = str(self.path / relpath)
        return self.fs.rm(path, recursive=recursive)
