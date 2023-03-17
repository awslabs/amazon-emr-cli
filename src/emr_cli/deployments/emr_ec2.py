from typing import List, Optional

import boto3

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
        response = self.client.add_job_flow_steps(
            JobFlowId=self.cluster_id,
            Steps=[
                {
                    "Name": "emr-cli job",
                    "ActionOnFailure": "CONTINUE",
                    "HadoopJarStep": {
                        "Jar": "command-runner.jar",
                        "Args": [
                            "spark-submit",
                            "--deploy-mode",
                            "cluster",
                            self.dp.entrypoint_uri(),
                        ]
                        + self.dp.spark_submit_parameters()
                        .params_for("emr_ec2")
                        .split(" "),
                    },
                }
            ],
        )
        step_id = response.get("StepIds")[0]
        console_log(f"Job submitted to EMR on EC2 (Step ID: {step_id})")
