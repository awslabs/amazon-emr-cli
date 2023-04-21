import gzip
import sys
from os.path import join
from typing import List, Optional

import boto3
from botocore.exceptions import ClientError, WaiterError
from emr_cli.deployments.emr_serverless import DeploymentPackage
from emr_cli.utils import console_log, parse_bucket_uri, print_s3_gz

LOG_WAITER_DELAY_SEC = 30


class EMREC2:
    def __init__(
        self, cluster_id: str, deployment_package: DeploymentPackage, region: str = ""
    ) -> None:
        self.cluster_id = cluster_id
        self.dp = deployment_package
        self.client = boto3.client("emr")
        self.s3_client = boto3.client("s3")

    def run_job(
        self,
        job_name: str,
        job_args: Optional[List[str]] = None,
        wait: bool = True,
        show_logs: bool = False,
    ):
        """
        Run a Spark job on EMR on EC2. Some important notes:
        1. --deploy-mode cluster is important for distributing dependencies
        2. entrypoint script must be the last argument
        3. show_logs implies `wait=True`
        """
        deploy_mode = "client" if show_logs else "cluster"
        spark_submit_params = self.dp.spark_submit_parameters().params_for("emr_ec2")

        # show_logs is only compatible with client mode
        # --conf spark.archives is only compatible with cluster mode
        # So if we have both, we have to throw an error
        # See https://issues.apache.org/jira/browse/SPARK-36088
        if (
            "--conf spark.archives" in spark_submit_params
            or "--archives" in spark_submit_params
        ):
            raise RuntimeError(
                "--show-stdout is not compatible with projects that make use of "
                + "--archives.\nPlease 👍 this GitHub issue to voice your support: "
                + "https://github.com/awslabs/amazon-emr-cli/issues/12"
            )

        try:
            response = self.client.add_job_flow_steps(
                JobFlowId=self.cluster_id,
                Steps=[
                    {
                        "Name": job_name,
                        "ActionOnFailure": "CONTINUE",
                        "HadoopJarStep": {
                            "Jar": "command-runner.jar",
                            "Args": [
                                "spark-submit",
                                "--deploy-mode",
                                deploy_mode,
                            ]
                            + spark_submit_params.split(" ")
                            + [self.dp.entrypoint_uri()],
                        },
                    }
                ],
            )
        except ClientError as err:
            console_log(err)
            sys.exit(1)

        step_id = response.get("StepIds")[0]
        console_log(f"Job submitted to EMR on EC2 (Step ID: {step_id})")
        if not wait and not show_logs:
            return step_id

        console_log("Waiting for step to complete...")
        waiter = self.client.get_waiter("step_complete")
        job_failed = False
        try:
            waiter.wait(
                ClusterId=self.cluster_id,
                StepId=step_id,
            )
            console_log("Job completed successfully!")
        except WaiterError:
            console_log("EMR on EC2 step failed!")
            job_failed = True  # So we can exit(1) later
            if not show_logs:
                sys.exit(1)

        if show_logs:
            # We need to validate s3-logging is enabled and fetch the location of the logs
            try:
                logs_location = self._fetch_log_location()
                stdout_location = self._wait_for_logs(step_id, logs_location, 30 * 60)
                console_log(f"stdout for {step_id}\n{'-'*36}")
                print_s3_gz(self.s3_client, stdout_location)
                if job_failed:
                    sys.exit(1)
            except RuntimeError as e:
                console_log(f"ERR: {e}")
                sys.exit(1)
            except WaiterError as e:
                console_log(f"ERR: While waiting for logs to appear: {e}")

        return step_id

    def _fetch_log_location(self) -> str:
        """
        Fetch the cluster and ensure it has the loguri set,
        then return the s3 location.
        """
        cluster_info = self.client.describe_cluster(ClusterId=self.cluster_id)
        loguri = cluster_info.get("Cluster").get("LogUri")
        if loguri is None:
            raise RuntimeError("Cluster does not have S3 logging enabled")
        return loguri.replace("s3n:", "s3:")

    def _wait_for_logs(self, step_id: str, log_base: str, timeout_secs: int) -> str:
        """
        Waits for stdout logs to appear in S3. Checks every LOG_WAITER_DELAY_SEC seconds
        until `timeout_secs`.
        """
        object_name = join(log_base, self.cluster_id, "steps", step_id, "stdout.gz")
        console_log(f"Waiting for logs to appear in {object_name} ...")
        bucket_name, key = parse_bucket_uri(object_name)
        waiter = self.s3_client.get_waiter("object_exists")
        waiter.wait(
            Bucket=bucket_name,
            Key=key,
            WaiterConfig={
                "Delay": LOG_WAITER_DELAY_SEC,
                "MaxAttempts": timeout_secs / LOG_WAITER_DELAY_SEC,
            },
        )
        return object_name
