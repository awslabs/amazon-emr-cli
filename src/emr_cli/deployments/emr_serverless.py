import abc
import os
import sys
from time import sleep
from typing import List, Optional

import boto3

from emr_cli.utils import console_log


class DeploymentPackage(metaclass=abc.ABCMeta):
    def __init__(
        self, entry_point_path: str = "entrypoint.py", s3_target_uri: str = ""
    ) -> None:
        self.entry_point_path = entry_point_path
        self.dist_dir = "dist"

        # We might not populate this until we actually deploy
        self.s3_uri_base = s3_target_uri

    def spark_submit_parameters(self) -> str:
        """
        Returns any additional arguments necessary for spark-submit
        """
        return ""

    def entrypoint_uri(self) -> str:
        """
        Returns the full S3 URI to the entrypoint file, e.g. s3://bucket/path/somecode.py
        """
        if self.s3_uri_base is None:
            raise Exception("S3 URI has not been set, aborting")
        return os.path.join(self.s3_uri_base, self.entry_point_path)


class EMRServerless:
    def __init__(
        self,
        application_id: str,
        job_role: str,
        deployment_package: DeploymentPackage,
        region: str = "",
    ) -> None:
        self.application_id = application_id
        self.job_role = job_role
        self.dp = deployment_package
        if region:
            self.client = boto3.client("emr-serverless", region_name=region)
        else:
            # Note that boto3 uses AWS_DEFAULT_REGION, not AWS_REGION
            # We may want to add an extra check here for the latter.
            self.client = boto3.client("emr-serverless")

    def run_job(
        self,
        job_name: str,
        job_args: Optional[List[str]] = None,
        spark_submit_opts: Optional[str] = None,
        wait: bool = True,
    ):
        jobDriver = {
            "sparkSubmit": {
                "entryPoint": self.dp.entrypoint_uri(),
            }
        }
        spark_submit_parameters = ""

        if len(self.dp.spark_submit_parameters()) > 0:
            spark_submit_parameters = self.dp.spark_submit_parameters().strip()

        if spark_submit_opts:
            spark_submit_parameters += f" {spark_submit_opts}".strip()

        if spark_submit_parameters:
            jobDriver["sparkSubmit"]["sparkSubmitParameters"] = spark_submit_parameters

        if job_args:
            jobDriver["sparkSubmit"]["entryPointArguments"] = job_args  # type: ignore

        response = self.client.start_job_run(
            applicationId=self.application_id,
            executionRoleArn=self.job_role,
            name=job_name,
            jobDriver=jobDriver,
            # configurationOverrides={
            #     "monitoringConfiguration": {
            #         "s3MonitoringConfiguration": {
            #             "logUri": "s3://<BUCKET>/logs/"
            #         }
            #     }
            # },
        )
        job_run_id = response.get("jobRunId")

        console_log(f"Job submitted to EMR Serverless (Job Run ID: {job_run_id})")
        if wait:
            console_log("Waiting for job to complete...")

        job_done = False
        job_state = "SUBMITTED"
        jr_response = {}
        while wait and not job_done:
            jr_response = self.get_job_run(job_run_id)
            new_state = jr_response.get("state")
            if new_state != job_state:
                console_log(f"Job state is now: {new_state}")
                job_state = new_state
            job_done = new_state in [
                "SUCCESS",
                "FAILED",
                "CANCELLING",
                "CANCELLED",
            ]
            sleep(2)

        if wait:
            if jr_response.get("state") != "SUCCESS":
                console_log(
                    f"EMR Serverless job failed: {jr_response.get('stateDetails')}"
                )
                sys.exit(1)
            console_log("Job completed successfully!")

        return job_run_id

    def get_job_run(self, job_run_id: str) -> dict:
        response = self.client.get_job_run(
            applicationId=self.application_id, jobRunId=job_run_id
        )
        return response.get("jobRun")
