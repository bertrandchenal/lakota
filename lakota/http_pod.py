import requests

from .pod import POD
from .utils import logger


class HttpPOD(POD):

    protocol = "http"

    def __init__(self, base_uri, path=None, session=None):
        self.base_uri = base_uri
        self.path = path
        self.session = session or requests.Session()
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
