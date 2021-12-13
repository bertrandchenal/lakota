import base64
from pathlib import PurePosixPath

import requests

from .pod import POD
from .utils import logger


class HttpPOD(POD):

    protocol = "http"

    def __init__(self, base_uri, path=None, session=None, headers=None):
        self.base_uri = base_uri if base_uri.endswith("/") else base_uri + "/"
        self.path = path or PurePosixPath("")
        if session:
            self.session = session
        else:
            self.session = requests.Session()
            self.session.headers.update(headers)
        super().__init__()

    def cd(self, *others):
        path = self.path.joinpath(*others)
        return HttpPOD(self.base_uri, path, session=self.session)

    def ls(self, relpath=".", missing_ok=False):
        logger.debug("LIST %s %s %s", self.base_uri, self.path, relpath)
        params = {"path": str(self.path / relpath)}
        resp = self.session.get(self.base_uri + "ls", params=params)

        if resp.status_code == 404:
            if missing_ok:
                return []
            raise FileNotFoundError(f"{relpath} not found")
        else:
            resp.raise_for_status()
        return resp.json()["body"]

    def read(self, relpath, mode="rb"):
        logger.debug("READ %s://%s %s", self.protocol, self.path, relpath)
        params = {"path": str(self.path / relpath)}
        resp = self.session.get(self.base_uri + "read", params=params)

        if resp.status_code == 404:
            raise FileNotFoundError(f"{relpath} not found")
        else:
            resp.raise_for_status()
        return base64.b64decode(resp.json()["body"])

    def write(self, relpath, data, mode="wb", force=False):
        logger.debug("WRITE %s://%s %s", self.protocol, self.path, relpath)
        path = str(self.path / relpath)
        params = {"path": str(path), "force": "true" if force else ""}
        resp = self.session.post(self.base_uri + "write", params=params, data=data)
        resp.raise_for_status()
        body = resp.json()["body"]
        return body

    def rm(self, relpath=".", recursive=False, missing_ok=False):
        logger.debug("REMOVE %s://%s %s", self.protocol, self.path, relpath)
        path = str(self.path / relpath)
        params = {
            "recursive": "true" if recursive else "",
            "missing_ok": "true" if missing_ok else "",
            "path": path,
        }
        resp = self.session.post(self.base_uri + "rm", params=params)
        if resp.status_code == 404:
            if missing_ok:
                return
            else:
                raise FileNotFoundError(f"{relpath} not found")
        resp.raise_for_status()

    def mv(self, from_path, to_path, missing_ok=False):
        orig = str(self.path / from_path)
        dest = str(self.path / to_path)
        logger.debug(
            "MOVE %s://%s to %s://%s", self.protocol, orig, self.protocol, dest
        )
        params = {
            "from_path": orig,
            "to_path": dest,
            "missing_ok": "true" if missing_ok else "",
        }
        resp = self.session.post(self.base_uri + "mv", params=params)
        if resp.status_code == 404:
            raise FileNotFoundError(f"{from_path} not found")
        else:
            resp.raise_for_status()

    def walk(self, max_depth=None):
        if max_depth == 0:
            return []

        params = {"path": str(self.path)}
        if max_depth is not None:
            params["max_depth"] = str(max_depth)

        resp = self.session.get(self.base_uri + "walk", params=params)
        resp.raise_for_status()
        body = resp.json()["body"]
        return body
