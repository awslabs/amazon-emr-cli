import os
import subprocess
import sys
from pathlib import Path
from shutil import copy

import boto3

from emr_cli.deployments import SparkParams
from emr_cli.deployments.emr_serverless import DeploymentPackage
from emr_cli.utils import (
    PrettyUploader,
    console_log,
    copy_template,
    parse_bucket_uri,
    validate_build_target,
)


class PythonProject(DeploymentPackage):
    def initialize(self, target_dir: str = os.getcwd()):
        """
        Initializes a pyspark project in the provided directory.
        - Creates a basic project
        - Creates a pyproject.toml file
        - Creates a Dockerfile
        """
        console_log(f"Initializing project in {target_dir}")
        copy_template("pyspark", target_dir)
        console_log("Project initialized.")

    def copy_single_file(self, relative_file_path: str, target_dir: str = os.getcwd()):
        """
        Copies a single file from the template directory to the target directory.
        """
        template_path = (
            Path(__file__).parent.parent / "templates" / "pyspark" / relative_file_path
        )
        target_path = Path(target_dir)
        copy(template_path, target_path)

    def build(self):
        """
        For now, uses a pre-existing Docker file and setuptools
        """
        if not Path("Dockerfile").exists():
            print(
                "Error: No Dockerfile present, use 'emr-cli init --dockerfile' to generate one"  # noqa: E501
            )
            sys.exit(1)
        if not Path("pyproject.toml").exists():
            print("Error: No pyproject.toml present, please set one up before building")
            sys.exit(1)

        console_log(f"Packaging assets into {self.dist_dir}/")
        self._run_docker_build(self.dist_dir)

    def _run_docker_build(self, output_dir: str):
        validate_build_target("export-python")
        subprocess.run(
            [
                "docker",
                "build",
                "--target",
                "export-python",
                "--output",
                output_dir,
                ".",
            ],
            check=True,
            env=dict(os.environ, DOCKER_BUILDKIT="1"),
        )

    def deploy(self, s3_code_uri: str) -> str:
        """
        Copies local code to S3 and returns the path to the uploaded entrypoint
        """
        self.s3_uri_base = s3_code_uri
        s3_client = boto3.client("s3")
        bucket, prefix = parse_bucket_uri(self.s3_uri_base)
        filename = os.path.basename(self.entry_point_path)

        console_log(f"Deploying {filename} and dependencies to {self.s3_uri_base}")

        uploader = PrettyUploader(
            s3_client,
            bucket,
            {
                self.entry_point_path: os.path.join(prefix, filename),
                os.path.join(self.dist_dir, "pyspark_deps.tar.gz"): os.path.join(
                    prefix, "pyspark_deps.tar.gz"
                ),
            },
        )
        uploader.run()

        return f"s3://{bucket}/{prefix}/{filename}"

    def spark_submit_parameters(self) -> SparkParams:
        tar_path = os.path.join(self.s3_uri_base, "pyspark_deps.tar.gz")
        return SparkParams(
            common_params={
                "spark.archives": f"{tar_path}#environment",
            },
            emr_serverless_params={
                "spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON": "./environment/bin/python",
                "spark.emr-serverless.driverEnv.PYSPARK_PYTHON": "./environment/bin/python",
                "spark.executorEnv.PYSPARK_PYTHON": "./environment/bin/python",
            },
            emr_ec2_params={
                "spark.executorEnv.PYSPARK_PYTHON": "./environment/bin/python",
                "spark.yarn.appMasterEnv.PYSPARK_PYTHON": "./environment/bin/python",
            },
            emr_eks_params={
                "spark.pyspark.python": "./environment/bin/python",
            },
        )
