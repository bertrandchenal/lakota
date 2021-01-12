from flask import Flask, Response, abort, request

app = Flask("Lakota Repository")


@app.route("/<action>/")
@app.route("/<action>/<path:relpath>", methods=["GET", "POST"])
def pod(action, relpath=None):
    repo = app.config["lakota_repo"]
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


def run(repo, netloc=None, debug=False):
    host, port = netloc.split(":", 1)
    port = port and int(port) or None
    app.config["lakota_repo"] = repo
    app.run(host, debug=debug, port=port)
