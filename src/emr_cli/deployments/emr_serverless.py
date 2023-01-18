import os
import sys
from time import sleep
from typing import List
import boto3

from emr_cli.utils import console_log


class EMRServerless:
    def __init__(
        self,
        application_id: str,
        s3_artifacts_uri: str,
        entry_point: str,
        job_role: str,
        region: str = None,
    ) -> None:
        self.application_id = application_id
        self.entry_point = os.path.basename(entry_point)
        self.s3_artifacts_uri = s3_artifacts_uri
        self.job_role = job_role
        if region:
            self.client = boto3.client("emr-serverless", region_name=region)
        else:
            # Note that boto3 uses AWS_DEFAULT_REGION, not AWS_REGION
            # We may want to add an extra check here for the latter.
            self.client = boto3.client("emr-serverless")

    def run_job(self, job_name: str, job_args: List[str] = None, wait: bool = True):
        s3_entrypoint_uri = os.path.join(self.s3_artifacts_uri, self.entry_point)
        s3_archives_uri = os.path.join(self.s3_artifacts_uri, "pyspark_deps.tar.gz")
        jobDriver = {
            "sparkSubmit": {
                "entryPoint": s3_entrypoint_uri,
                "sparkSubmitParameters": f"--conf spark.archives={s3_archives_uri}#environment --conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python --conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python --conf spark.executorEnv.PYSPARK_PYTHON=./environment/bin/python",
            }
        }
        if job_args:
            jobDriver["sparkSubmit"]["entryPointArguments"] = job_args

        response = self.client.start_job_run(
            applicationId=self.application_id,
            executionRoleArn=self.job_role,
            name=job_name,
            jobDriver=jobDriver,
            # configurationOverrides={
            #     "monitoringConfiguration": {
            #         "s3MonitoringConfiguration": {
            #             "logUri": f"s3://{s3_bucket_name}/{self.s3_log_prefix}"
            #         }
            #     }
            # },
        )
        job_run_id = response.get("jobRunId")

        console_log(f"Job submitted to EMR Serverless (Job Run ID: {job_run_id})")
        if wait:
            console_log(f"Waiting for job to complete...")

        job_done = False
        job_state = "SUBMITTED"
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
            console_log(f"Job completed successfully!")

        return job_run_id

    def get_job_run(self, job_run_id: str) -> dict:
        response = self.client.get_job_run(
            applicationId=self.application_id, jobRunId=job_run_id
        )
        return response.get("jobRun")
