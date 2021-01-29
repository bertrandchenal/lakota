"""
The server sub-module implement a flask application that expose
the main methods of `lakota.pod.POD`.  It can be launched from the cli
like this:

``` shell
$ lakota serve
 * Serving Flask app "Lakota Repository" (lazy loading)
 * Environment: production
   WARNING: This is a development server. Do not use it
   in a production deployment.
   Use a production WSGI server instead.
 * Debug mode: off
INFO:2021-01-22 16:04:08:  * Running on http://127.0.0.1:8080/ (Press CTRL+C to quit)
```

In the above example the repository exposed is the usual default
repository of the cli (so the folder `".lakota"` or the one defined in
the environment variable `LAKOTA_REPO` if this one is set up).

Once the server is started you can query it, for example you can do
(while the server is running in another shell):

``` shell
$ lakota -r http://localhost:8080 ls
...
```

It can also be used as a local proxy to speed-up access to remote locations:

``` shell
$ lakota -r memory://+s3:///bucket_name serve
```

You can also use `file:////tmp/local-cache` instead of `memory://` to
provide persistant caching.

**Beware**: no authentication nor encryption is provided, and the server
expose full read and write access to the underlying repository.
"""

from urllib.parse import urlsplit

from flask import Blueprint, Flask, Response, abort, request

# Simple dict to register repositories
dispatcher = {}

pod_bp = Blueprint(f"Lakota POD", __name__)


@pod_bp.route("/<action>", methods=["GET", "POST"])
def pod(repo, action, relpath=None):
    """
    summary: Interact with low-level POD object (GET)
    ---
    parameters:
      - in: action
        name: action
        schema:
          type: string
        required: true
        description: Action to perform (ls, read, rm or walk)
      - in: path
        name: relpath
        schema:
          type: string
        required: false
        description: Relative path
    """
    relpath = request.args.get("path")

    if action == "ls":
        try:
            relpath = "." if relpath is None else relpath
            payload = "\n".join(repo.pod.ls(relpath))
        except FileNotFoundError:
            return abort(404)
        return Response(payload, mimetype="text/plain")

    elif action == "read":
        payload = repo.pod.read(relpath)
        return Response(payload, mimetype="application/octet-stream")

    elif action == "rm":
        recursive = request.args.get("recursive", "").lower() == "true"
        missing_ok = request.args.get("missing_ok", "").lower() == "true"
        repo.pod.rm(relpath, recursive=recursive, missing_ok=missing_ok)
        return Response("ok", mimetype="text/plain")

    elif action == "write":
        info = repo.pod.write(relpath, request.data)
        return Response(str(info or ""), mimetype="text/plain")

    elif action == "walk":
        pod = repo.pod
        if relpath:
            pod = pod.cd(relpath)
        max_depth = request.args.get("max_depth")
        if max_depth is not None:
            max_depth = int(max_depth)
        payload = "\n".join(pod.walk(max_depth=max_depth))
        return Response(payload, mimetype="text/plain")

    else:
        return abort(404, f"Action {action} not supported")


def run(repo, web_uri=None, debug=False):
    parts = urlsplit(web_uri)
    if not parts.scheme == "http":
        # if no scheme is given, hostname and port are not interpreted correctly
        msg = "Incorrect web uri, it should start with 'http://'"
        raise ValueError(msg)

    # Instanciate app and blueprint. Run app
    app = Flask("Lakota Repository")
    app.register_blueprint(pod_bp, url_prefix=parts.path, url_defaults={"repo": repo})
    app.run(parts.hostname, debug=debug, port=parts.port)
