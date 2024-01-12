import re
import sys
from os.path import join
from platform import release
from time import sleep
from typing import List, Optional

import boto3
from emr_cli.deployments.emr_serverless import DeploymentPackage
from emr_cli.utils import console_log, print_s3_gz


class EMREKS:
    def __init__(
        self, virtual_cluster_id: str, job_role: str, deployment_package: DeploymentPackage, region: str = ""
    ) -> None:
        self.virtual_cluster_id = virtual_cluster_id
        self.job_role = job_role
        self.dp = deployment_package
        self.s3_client = boto3.client("s3")
        if region:
            self.client = boto3.client("emr-containers", region_name=region)
            self.emr_client = boto3.client("emr", region_name=region)
        else:
            # Note that boto3 uses AWS_DEFAULT_REGION, not AWS_REGION
            # We may want to add an extra check here for the latter.
            self.client = boto3.client("emr-containers")
            self.emr_client = boto3.client("emr")

    def fetch_latest_release_label(self):
        response = self.emr_client.list_release_labels(
            Filters={"Application": "Spark", "Prefix": "emr-6"}, MaxResults=1
        )
        if len(response["ReleaseLabels"]) == 0:
            console_log("Error: No release labels found")
            sys.exit(1)
        return response["ReleaseLabels"][0]

    def run_job(
        self,
        job_name: str,
        job_args: Optional[List[str]] = None,
        spark_submit_opts: Optional[str] = None,
        wait: bool = True,
        show_logs: bool = False,
        s3_logs_uri: Optional[str] = None,
        release_label: Optional[str] = None,
    ):
        if show_logs and not s3_logs_uri:
            raise RuntimeError("--show-stdout requires --s3-logs-uri to be set.")

        if release_label is None:
            release_label = self.fetch_latest_release_label()
            console_log(f"Using latest release label {release_label}")
        release_label = f"{release_label}-latest"

        # If job_name is the default, just replace the space.
        # Otherwise throw an error
        if job_name == "emr-cli job":
            job_name = "emr-cli_job"
        elif not re.fullmatch("[\.\-_/#A-Za-z0-9]+", job_name):
            console_log(f"Invalid characters in job name {job_name} - EMR on EKS must match [\.\-_/#A-Za-z0-9]+")
            sys.exit(1)

        jobDriver = {
            "sparkSubmitJobDriver": {
                "entryPoint": self.dp.entrypoint_uri(),
            }
        }
        spark_submit_parameters = self.dp.spark_submit_parameters().params_for("emr_eks")

        if spark_submit_opts:
            spark_submit_parameters = f"{spark_submit_parameters} {spark_submit_opts}".strip()

        if spark_submit_parameters:
            jobDriver["sparkSubmitJobDriver"]["sparkSubmitParameters"] = spark_submit_parameters

        if job_args:
            jobDriver["sparkSubmitJobDriver"]["entryPointArguments"] = job_args  # type: ignore

        config_overrides = {}
        if s3_logs_uri:
            config_overrides = {"monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": s3_logs_uri}}}

        response = self.client.start_job_run(
            virtualClusterId=self.virtual_cluster_id,
            executionRoleArn=self.job_role,
            name=job_name,
            jobDriver=jobDriver,
            configurationOverrides=config_overrides,
            releaseLabel=release_label,
        )
        job_run_id = response.get("id")

        console_log(f"Job submitted to EMR Virtual Cluster (Job Run ID: {job_run_id})")
        if not wait and not show_logs:
            return job_run_id

        console_log("Waiting for job to complete...")
        job_done = False
        job_state = "SUBMITTED"
        jr_response = {}
        while not job_done:
            jr_response = self.get_job_run(job_run_id)
            new_state = jr_response.get("state")
            if new_state != job_state:
                console_log(f"Job state is now: {new_state}")
                job_state = new_state
            job_done = new_state in [
                "COMPLETED",
                "FAILED",
                "CANCEL_PENDING",
                "CANCELLED",
            ]
            sleep(2)

        if show_logs:
            console_log(f"stdout for {job_run_id}\n{'-'*38}")
            log_location = join(
                f"{s3_logs_uri}",
                self.virtual_cluster_id,
                "jobs",
                job_run_id,
                "containers",
                f"spark-{job_run_id}",
                f"spark-{job_run_id}-driver",
                "stdout.gz",
            )
            print_s3_gz(self.s3_client, log_location)

        if jr_response.get("state") != "COMPLETED":
            console_log(f"EMR Containers job failed: {jr_response.get('stateDetails')}")
            sys.exit(1)
        console_log("Job completed successfully!")

        return job_run_id

    def get_job_run(self, job_run_id: str) -> dict:
        response = self.client.describe_job_run(virtualClusterId=self.virtual_cluster_id, id=job_run_id)
        return response.get("jobRun")
