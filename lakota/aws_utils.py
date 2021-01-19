import os
import shutil
from subprocess import run
from tempfile import TemporaryDirectory

import boto3

from lakota.utils import logger


def deploy_lambda(name, role_arn, lakota_package="lakota"):
    # Create zip
    extra_packages = ["aws-wsgi", "flask", "requests"]
    with TemporaryDirectory() as tdir:
        args = ["pip", "install", "-t", tdir, lakota_package] + extra_packages
        run(args)
        shutil.make_archive("lambda", "zip", tdir)

    env_variables = {
        "LAKOTA_REPO": os.environ["LAKOTA_REPO"],
    }
    with open("lambda.zip", "rb") as f:
        zipped_code = f.read()

    # Delete function if it already exists
    lambda_client = boto3.client("lambda")
    try:
        lambda_client.delete_function(
            FunctionName=name,
        )
        logger.info("Function %s deleted", name)
    except lambda_client.exceptions.ResourceNotFoundException:
        print("NOT FOUND")
        pass

    # Create function
    lambda_client.create_function(
        FunctionName=name,
        Runtime="python3.7",
        # Role=role['Role']['Arn'],
        Role=role_arn,
        Handler="lakota.aws_app.lambda_handler",
        Code=dict(ZipFile=zipped_code),
        Timeout=30,  # Maximum allowable timeout
        Environment=dict(Variables=env_variables),
    )
