import os
import zipfile

import boto3
from emr_cli.deployments import SparkParams
from emr_cli.deployments.emr_serverless import DeploymentPackage
from emr_cli.utils import (
    PrettyUploader,
    console_log,
    find_files,
    mkdir,
    parse_bucket_uri,
)


class PythonFilesProject(DeploymentPackage):
    """
    A PythonFilesProject is a simple project that includes multiple `.py` files.

    This is a simple project that has no external dependencies and requires no
    additional packaging. The files in the project are simply zipped up.
    """

    def build(self):
        """
        Zip all the files except for the entrypoint file.
        """
        py_files = find_files(os.getcwd(), [".venv"], ".py")
        py_files.remove(os.path.abspath(self.entry_point_path))
        cwd = os.getcwd()
        mkdir(self.dist_dir)
        with zipfile.ZipFile(f"{self.dist_dir}/pyfiles.zip", "w") as zf:
            for file in py_files:
                relpath = os.path.relpath(file, cwd)
                zf.write(file, relpath)

    def deploy(self, s3_code_uri: str) -> str:
        """
        Copies local code to S3 and returns the path to the uploaded entrypoint
        """
        s3_client = boto3.client("s3")
        bucket, prefix = parse_bucket_uri(s3_code_uri)
        filename = os.path.basename(self.entry_point_path)

        console_log(f"Deploying {filename} and local python modules to {s3_code_uri}")

        uploader = PrettyUploader(
            s3_client,
            bucket,
            {
                self.entry_point_path: os.path.join(prefix, filename),
                os.path.join(self.dist_dir, "pyfiles.zip"): os.path.join(
                    prefix, "pyfiles.zip"
                ),
            },
        )
        uploader.run()

        return f"s3://{bucket}/{prefix}/{filename}"

    def spark_submit_parameters(self) -> SparkParams:
        zip_path = os.path.join(self.s3_uri_base, "pyfiles.zip")
        return SparkParams(
            common_params={
                "spark.submit.pyFiles": zip_path,
            },
        )
