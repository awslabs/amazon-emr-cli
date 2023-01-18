import os
import shutil
import subprocess
import sys
from pathlib import Path

from typing import List
from urllib.parse import urlparse

import boto3

from emr_cli.utils import console_log


class PythonProject:
    def __init__(self, entry_point_path: str = "entrypoint.py") -> None:
        self.entry_point_path = entry_point_path

    def initialize(self, target_dir: str = os.getcwd()):
        """
        Initializes a pyspark project in the current directory.
        - Creates a basic project
        - Creates a pyproject.toml file
        - Creates a Dockerfile.build
        - Creates a Dockerfile
        """
        console_log(f"Initializing project in {target_dir}")
        self._copy_template()
        console_log("Project initialized.")

    def _copy_template(self):
        source = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "templates", "pyspark")
        )
        target = os.getcwd()
        shutil.copytree(source, target, dirs_exist_ok=True)

    def build(self):
        """
        For now, uses a pre-existing Docker file and setuptools
        """
        if not Path("Dockerfile").exists():
            print(
                "Error: No Dockerfile present, use 'emr-cli init --dockerfile' to generate one"
            )
            sys.exit(1)
        if not Path("pyproject.toml").exists():
            print("Error: No pyproject.toml present, please set one up before building")
            sys.exit(1)

        console_log(f"Packaging assets into dist/")
        self._run_docker_build("dist")

    def _run_docker_build(self, output_dir: str):
        docker_build = subprocess.run(
            ["docker", "build", "--output", output_dir, "."],
            check=True,
            env=dict(os.environ, DOCKER_BUILDKIT="1"),
        )

    def deploy(self, s3_code_uri: str) -> None:
        """
        Copies local code to S3 and returns the path to the uploaded entrypoint
        """
        s3_client = boto3.client("s3")
        bucket, prefix = self._parse_bucket_uri(s3_code_uri)
        filename = os.path.basename(self.entry_point_path)

        console_log(f"Deploying {filename} to {s3_code_uri}")

        s3_client.upload_file(self.entry_point_path, bucket, f"{prefix}/{filename}")
        s3_client.upload_file(
            "dist/pyspark_deps.tar.gz", bucket, f"{prefix}/pyspark_deps.tar.gz"
        )

        return f"s3://{bucket}/{prefix}/{filename}"

    def _parse_bucket_uri(self, uri: str) -> List[str]:
        result = urlparse(uri, allow_fragments=False)
        return [result.netloc, result.path.strip("/")]
