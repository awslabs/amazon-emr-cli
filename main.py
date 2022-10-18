import os
from typing import Tuple

import boto3
import yaml


def config() -> dict:
    with open("conf/deployment.yaml", "r") as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def initialize_project(app_id: str):
    """
    Create a "conf" directory with a sample yaml deployment file including the app_id
    """
    config = {
        "default": {
            "application_id": app_id,
            "deploy_target": "s3://YOUR-BUCKET/code/pyspark/emr-tools/",
            "job_role_arn": "arn:aws:iam::YOUR-AWS-ACCOUNT:role/emr-serverless-job-role",
        }
    }
    # Check if "conf" dir exists
    if not os.path.exists("conf"):
        os.mkdir("conf")
        with open("conf/deployment.yaml", "w") as f:
            yaml.dump(config, f)


def deploy_project():
    # If there is a requirements.txt file, we should create a virtualenv and upload that (and the code)
    # to S3
    if os.path.exists("requirements.txt"):
        exit_code = os.system("docker build --output ./dist .")
        if exit_code != 0:
            raise Exception("docker build failed")
        upload_file_to_s3(
            "./dist/pyspark_deps.tar.gz",
            config().get("default").get("deploy_target"),
        )
    # Good for now - we assume we'll use a demo pi.py script


def run_job():
    client = boto3.client("emr-serverless")
    conf = config().get("default")
    dependency_location = f"{conf.get('deploy_target')}/pyspark_deps.tar.gz"

    response = client.start_job_run(
        applicationId=conf.get("application_id"),
        executionRoleArn=conf.get("job_role_arn"),
        jobDriver={
            "sparkSubmit": {
                "entryPoint": "pi.py",
                "sparkSubmitParameters": f"--conf spark.archives={dependency_location}#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.emr-serverless.executorEnv.PYSPARK_PYTHON=./environment/bin/python",
            }
        },
        name="sample-job",
    )
    print(response)


def upload_file_to_s3(path: str, s3_uri: str):
    client = boto3.client("s3")
    bucket, prefix = get_s3_parts(s3_uri)
    file_name = os.path.basename(path)
    client.upload_file(path, bucket, f"{prefix}{file_name}")


def get_s3_parts(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise Exception("Invalid S3 URI for deploy target - does not start with s3://")

    bucket, prefix = uri.lstrip("s3://").split("/", 1)
    if not prefix.endswith("/"):
        prefix = f"{prefix}/"

    return bucket, prefix


if __name__ == "__main__":
    # initialize_project("1234")
    # deploy_project()
    run_job()
