import os
import subprocess
import sys
from pathlib import Path
from typing import List
from urllib.parse import urlparse

import boto3
from emr_cli.deployments.emr_serverless import DeploymentPackage
from emr_cli.utils import console_log


class PythonPoetryProject(DeploymentPackage):
    def build(self):
        if not Path("poetry.lock").exists():
            print("Error: No poetry.lock present, please setup your poetry project.")
            sys.exit(1)

        console_log(f"Packaging assets into {self.dist_dir}/")
        # TODO: Add an option for --force-local-build
        self._run_docker_build(self.dist_dir)

    def _run_local_build(self, output_dir: str = "dist"):
        subprocess.run(
            ["poetry", "bundle", "venv" "poeticemrbundle" "--without" "dev"], check=True
        )

    def _run_docker_build(self, output_dir: str):
        subprocess.run(
            [
                "docker",
                "build",
                "--target",
                "export-poetry",
                "--output",
                output_dir,
                "--file",
                self._dockerfile_path(),
                ".",
            ],
            check=True,
            env=dict(os.environ, DOCKER_BUILDKIT="1"),
        )
    
    def _dockerfile_path(self) -> str:
        if Path("Dockerfile").is_file():
            return "Dockerfile"
        
        templates = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "templates", "pyspark")
        )
        return os.path.join(templates, "Dockerfile")

    def deploy(self, s3_code_uri: str) -> str:
        """
        Copies local code to S3 and returns the path to the uploaded entrypoint
        """
        s3_client = boto3.client("s3")
        bucket, prefix = self._parse_bucket_uri(s3_code_uri)
        filename = os.path.basename(self.entry_point_path)

        console_log(f"Deploying {filename} and dependencies to {s3_code_uri}")

        s3_client.upload_file(
            self.entry_point_path, bucket, os.path.join(prefix, filename)
        )
        s3_client.upload_file(
            os.path.join(self.dist_dir, "pyspark_deps.tar.gz"),
            bucket,
            os.path.join(prefix, "pyspark_deps.tar.gz"),
        )

        return f"s3://{bucket}/{prefix}/{filename}"

    def _parse_bucket_uri(self, uri: str) -> List[str]:
        result = urlparse(uri, allow_fragments=False)
        return [result.netloc, result.path.strip("/")]
