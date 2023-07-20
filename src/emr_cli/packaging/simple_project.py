import os

import boto3

from emr_cli.deployments.emr_serverless import DeploymentPackage
from emr_cli.utils import PrettyUploader, console_log, parse_bucket_uri


class SimpleProject(DeploymentPackage):
    """
    A simple project only has a single entry point file.
    This can be a pyspark file or packaged jar file.
    """

    def build(self):
        pass

    def deploy(self, s3_code_uri: str) -> str:
        """
        Copies local code to S3 and returns the path to the uploaded entrypoint
        """
        s3_client = boto3.client("s3")
        bucket, prefix = parse_bucket_uri(s3_code_uri)
        filename = os.path.basename(self.entry_point_path)

        console_log(f"Deploying {filename} to {s3_code_uri}")
        uploader = PrettyUploader(
            s3_client,
            bucket,
            {
                self.entry_point_path: os.path.join(prefix, filename),
            },
        )
        uploader.run()

        return f"s3://{bucket}/{prefix}/{filename}"
