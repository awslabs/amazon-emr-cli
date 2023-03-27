import sys
from typing import List, Optional

import boto3
from botocore.exceptions import ClientError, WaiterError

from emr_cli.deployments.emr_serverless import DeploymentPackage
from emr_cli.utils import console_log


class EMREC2:
    def __init__(
        self, cluster_id: str, deployment_package: DeploymentPackage, region: str = ""
    ) -> None:
        self.cluster_id = cluster_id
        self.dp = deployment_package
        self.client = boto3.client("emr")

    def run_job(
        self, job_name: str, job_args: Optional[List[str]] = None, wait: bool = True
    ):
        """
        Run a Spark job on EMR on EC2. Some important notes:
        1. --deploy-mode cluster is important for distributing dependencies
        2. entrypoint script must be the last argument
        """
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
                                "cluster",
                            ]
                            + self.dp.spark_submit_parameters()
                            .params_for("emr_ec2")
                            .split(" ")
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
        if not wait:
            return step_id

        console_log("Waiting for step to complete...")
        waiter = self.client.get_waiter("step_complete")
        try:
            waiter.wait(
                ClusterId=self.cluster_id,
                StepId=step_id,
            )
        except WaiterError:
            console_log("EMR on EC2 step failed, exiting.")
            sys.exit(1)

        console_log("Job completed successfully!")
        # step_response = self.client.describe_step(
        #     ClusterId=self.cluster_id,
        #     StepId=step_id,
        # )

        return step_id
