from pathlib import Path

import s3fs
import urllib3

from .pod import POD
from .utils import logger


def silence_insecure_warning():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class S3POD(POD):

    protocol = "s3"

    def __init__(
        self,
        path,
        netloc=None,
        profile=None,
        verify=True,
        fs=None,
        key=None,
        secret=None,
        token=None,
    ):
        # TODO document use of param: endpoint_url='http://127.0.0.1:5300'
        self.path = path
        if fs:
            self.fs = fs
        else:
            if not verify:
                silence_insecure_warning()
            client_kwargs = {"verify": verify}
            if netloc:
                # TODO support for https
                client_kwargs["endpoint_url"] = f"http://{netloc}"
            self.fs = s3fs.S3FileSystem(
                anon=False,
                key=key,
                secret=secret,
                token=token,
                client_kwargs=client_kwargs,
                profile=profile,
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
