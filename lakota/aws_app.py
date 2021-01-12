import os

import awsgi

from lakota import Repo
from lakota.server import app

# Instanciate repo
app.config["lakota_repo"] = Repo(os.environ.get("LAKOTA_REPO"))


def lambda_handler(event, context):
    return awsgi.response(
        app, event, context, base64_content_types={"application/octet-stream"}
    )
