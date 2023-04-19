import gzip
import sys
from os.path import join
from typing import List, Optional

from botocore.exceptions import ClientError, WaiterError
from emr_cli.deployments.emr_serverless import DeploymentPackage
from emr_cli.utils import console_log, parse_bucket_uri
from emr_cli.base.EmrBase import EmrBase

LOG_WAITER_DELAY_SEC = 30


class EMREKS (EmrBase):
    def __init__(
        self, cluster_id: str, deployment_package: DeploymentPackage, region: str = "", profile: str = ""
    ) -> None:
        
        super().__init__(profile)
        
        aws_session = self.aws_session

        if region:
            self.client = aws_session.client("emr-containers", region_name=region)
        else:
            # Note that boto3 uses AWS_DEFAULT_REGION, not AWS_REGION
            # We may want to add an extra check here for the latter.
            self.client = aws_session.client("emr-containers")

        self.cluster_id = cluster_id
        self.dp = deployment_package
        
        self.s3_client = aws_session.client("s3")

    def run_job(
        self,
        job_name: str,
        job_args: Optional[List[str]] = None,
        wait: bool = True,
        show_logs: bool = False,
    ):
        """
        Start a Spark job on EMR on EKS.
        """
        spark_submit_params = self.dp.spark_submit_parameters().params_for("emr_eks")

        try:
            response = self.client.start_job_run(
                virtualClusterId=self.cluster_id,
                executionRoleArn='string',
                releaseLabel='string',
                jobDriver={
                    'sparkSubmitJobDriver': {
                        'entryPoint': 'string',
                        'entryPointArguments': [
                            'string',
                        ],
                        'sparkSubmitParameters': spark_submit_params
                    }
                },
            )
        except ClientError as err:
            console_log(err)
            sys.exit(1)

        step_id = response.get("id")[0]
        console_log(f"Job submitted to EMR on EKS (Job ID: {step_id})")
        if not wait and not show_logs:
            return id

        return step_id
