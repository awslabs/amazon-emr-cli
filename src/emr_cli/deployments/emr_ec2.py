import json
import shlex
import sys
import time
from os.path import join
from typing import List, Optional

import boto3
from botocore.exceptions import ClientError, WaiterError
from emr_cli.deployments.emr_serverless import DeploymentPackage
from emr_cli.utils import console_log, parse_bucket_uri, print_s3_gz

LOG_WAITER_DELAY_SEC = 30


class Bootstrap:
    DEFAULT_S3_POLICY_NAME = "emr-cli-S3Access"
    DEFAULT_GLUE_POLICY_NAME = "emr-cli-GlueAccess"

    def __init__(
        self,
        code_bucket: str,
        log_bucket: str,
        instance_role_name: str,
        job_role_name: str,
    ):
        self.code_bucket = code_bucket
        self.log_bucket = log_bucket or code_bucket
        self.instance_role_name = instance_role_name
        self.job_role_name = job_role_name
        self.s3_client = boto3.client("s3")
        self.iam_client = boto3.client("iam")
        self.emr_client = boto3.client("emr")

    def create_environment(self):
        self._create_s3_buckets()
        service_role_arn = self._create_service_role()

        # Make sure the role exists - there can be a tiny lag that will break setting up trust policies.
        # Unfortunately, using a waiter or querying or the role didn't help here.
        # There's a terraform issue about it here: https://github.com/hashicorp/terraform-provider-aws/issues/8905
        # It looks like the fix is just querying or the role, but that didn't work.
        time.sleep(10)
        print("Slept")

        job_role_arn = self._create_runtime_role(service_role_arn)

        # Allow the EC2 instance profile to assume the job role
        self.iam_client.put_role_policy(
            RoleName=self.instance_role_name,
            PolicyName="AssumeRuntimeRole",
            PolicyDocument=self._runtime_role_policy(job_role_arn),
        )

        security_config = self._create_security_config()  # "emr-cli-runtime-roles"
        cluster_id = self._create_cluster(security_config, self.instance_role_name)
        return {
            "cluster_id": cluster_id,
            "job_role_arn": job_role_arn,
            "code_bucket": self.code_bucket,
            "log_bucket": self.log_bucket,
        }

    def print_destroy_commands(self, cluster_id: str):
        # fmt: off
        print(f"aws emr terminate-clusters --cluster-ids {cluster_id}")
        print(f"aws emr wait cluster-terminated --cluster-id {cluster_id}")
        for bucket in set([self.log_bucket, self.code_bucket]):
            print(f"aws s3 rm s3://{bucket} --recursive")
            print(f"aws s3api delete-bucket --bucket {bucket}")
        print(f"aws iam remove-role-from-instance-profile --instance-profile-name {self.instance_role_name} --role-name {self.instance_role_name}")  # noqa E501
        print(f"aws iam delete-instance-profile --instance-profile-name {self.instance_role_name}")  # noqa E501
        for role_name in [self.instance_role_name, self.job_role_name]:
            for policy in self.iam_client.list_attached_role_policies(RoleName=role_name).get('AttachedPolicies'):  # noqa E501
                arn = policy.get('PolicyArn')
                print(f"aws iam detach-role-policy --role-name {role_name} --policy-arn {arn}")  # noqa E501
                print(f"aws iam delete-policy --policy-arn {arn}")  # noqa E501
            for name in self.iam_client.list_role_policies(RoleName=role_name).get('PolicyNames'):  # noqa E501
                print(f"aws iam delete-role-policy --role-name {role_name} --policy-name {name}")  # noqa E501
            print(f"aws iam delete-role --role-name {role_name}")
        print(f"aws emr delete-security-configuration --name emr-cli-runtime-roles")  # noqa E501
        # fmt: on

    def _create_s3_buckets(self):
        """
        Creates both the source and log buckets if they don't already exist.
        """
        for bucket_name in set([self.code_bucket, self.log_bucket]):
            self.s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": self.s3_client.meta.region_name},
            )
            console_log(f"Created S3 bucket: s3://{bucket_name}")
            self.s3_client.put_bucket_policy(Bucket=bucket_name, Policy=self._default_s3_bucket_policy(bucket_name))

    def _default_s3_bucket_policy(self, bucket_name) -> str:
        bucket_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "RequireSecureTransport",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:*",
                    "Resource": [f"arn:aws:s3:::{bucket_name}/*", f"arn:aws:s3:::{bucket_name}"],
                    "Condition": {
                        "Bool": {"aws:SecureTransport": "false", "aws:SourceArn": f"arn:aws:s3:::{bucket_name} "}
                    },
                }
            ],
        }
        return json.dumps(bucket_policy)

    def _create_service_role(self):
        """
        Create an EC2 instance profile and role for use with EMR
        https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-role-for-ec2.html
        """
        # First create a role that can be assumed by EC2
        response = self.iam_client.create_role(
            RoleName=self.instance_role_name,
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "ec2.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
        )
        role_arn = response.get("Role").get("Arn")
        console_log(f"Created IAM Role: {role_arn}")

        self.iam_client.create_instance_profile(InstanceProfileName=self.instance_role_name)
        self.iam_client.add_role_to_instance_profile(
            InstanceProfileName=self.instance_role_name,
            RoleName=self.instance_role_name,
        )
        return role_arn

    def _create_runtime_role(self, instance_profile_role_arn: str):
        response = self.iam_client.create_role(
            RoleName=self.job_role_name,
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"AWS": instance_profile_role_arn},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
        )
        role_arn = response.get("Role").get("Arn")
        console_log(f"Created IAM Role: {role_arn}")

        self.iam_client.attach_role_policy(RoleName=self.job_role_name, PolicyArn=self._create_s3_policy())
        self.iam_client.attach_role_policy(RoleName=self.job_role_name, PolicyArn=self._create_glue_policy())

        return role_arn

    def _create_s3_policy(self):
        bucket_arns = [f"arn:aws:s3:::{name}" for name in [self.code_bucket, self.log_bucket]]
        policy_doc = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "AllowListBuckets",
                    "Effect": "Allow",
                    "Action": ["s3:ListBucket"],
                    "Resource": bucket_arns,
                },
                {
                    "Sid": "WriteToCodeAndLogBuckets",
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
                    "Resource": [f"{arn}/*" for arn in bucket_arns],
                },
            ],
        }
        response = self.iam_client.create_policy(
            PolicyName=self.DEFAULT_S3_POLICY_NAME,
            PolicyDocument=json.dumps(policy_doc),
        )
        return response.get("Policy").get("Arn")

    def _create_glue_policy(self):
        policy_doc = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "GlueCreateAndReadDataCatalog",
                    "Effect": "Allow",
                    "Action": [
                        "glue:GetDatabase",
                        "glue:GetDataBases",
                        "glue:CreateTable",
                        "glue:GetTable",
                        "glue:GetTables",
                        "glue:GetPartition",
                        "glue:GetPartitions",
                        "glue:CreatePartition",
                        "glue:BatchCreatePartition",
                        "glue:GetUserDefinedFunctions",
                    ],
                    "Resource": "*",
                },
            ],
        }
        response = self.iam_client.create_policy(
            PolicyName=self.DEFAULT_GLUE_POLICY_NAME,
            PolicyDocument=json.dumps(policy_doc),
        )
        return response.get("Policy").get("Arn")

    def _runtime_role_policy(self, runtime_role_arn: str):
        return json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "AllowRuntimeRoleUsage",
                        "Effect": "Allow",
                        "Action": ["sts:AssumeRole", "sts:TagSession"],
                        "Resource": [runtime_role_arn],
                    }
                ],
            }
        )

    def _create_security_config(self):
        response = self.emr_client.create_security_configuration(
            Name="emr-cli-runtime-roles",
            SecurityConfiguration="""{
                "AuthorizationConfiguration":{
                    "IAMConfiguration":{
                        "EnableApplicationScopedIAMRole":true
                    }
                }
            }""",
        )
        return response.get("Name")

    def _create_cluster(self, security_config_name: str, instance_profile_name: str):
        """
        Create a simple Spark EMR on EC2 cluster.

        **WARNING** This cluster is only intended for demo/development purposes only.

        It is deployed in a public subnet by default and will auto-terminate in 4 hours.
        Runtime roles are enabled so you can submit jobs with the created job-role.

        To customize the cluster or create a cluster for production, use the AWS CLI
        or other Infrastructure as Code services like Terraform, CDK, or CloudFormation.
        """
        response = self.emr_client.run_job_flow(
            Name="emr-cli-demo",
            ReleaseLabel="emr-6.9.0",
            LogUri=f"s3://{self.log_bucket}/logs/emr/",
            Applications=[
                {"Name": "Spark"},
                {"Name": "Livy"},
                {"Name": "JupyterEnterpriseGateway"},
            ],
            AutoTerminationPolicy={"IdleTimeout": 14400},
            SecurityConfiguration=security_config_name,
            ServiceRole="EMR_DefaultRole",
            JobFlowRole=instance_profile_name,
            Instances={
                "KeepJobFlowAliveWhenNoSteps": True,
                "InstanceFleets": [
                    {
                        "Name": "Primary",
                        "InstanceFleetType": "MASTER",
                        "TargetOnDemandCapacity": 1,
                        "TargetSpotCapacity": 0,
                        "InstanceTypeConfigs": [
                            {"InstanceType": "r5.2xlarge"},
                            {"InstanceType": "r5b.2xlarge"},
                            {"InstanceType": "r5d.2xlarge"},
                            {"InstanceType": "r5a.2xlarge"},
                        ],
                    },
                    {
                        "Name": "Core",
                        "InstanceFleetType": "CORE",
                        "TargetOnDemandCapacity": 0,
                        "TargetSpotCapacity": 1,
                        "InstanceTypeConfigs": [
                            {"InstanceType": "c5a.2xlarge"},
                            {"InstanceType": "m5a.2xlarge"},
                            {"InstanceType": "r5a.2xlarge"},
                        ],
                        "LaunchSpecifications": {
                            "OnDemandSpecification": {"AllocationStrategy": "lowest-price"},
                            "SpotSpecification": {
                                "TimeoutDurationMinutes": 10,
                                "TimeoutAction": "SWITCH_TO_ON_DEMAND",
                                "AllocationStrategy": "capacity-optimized",
                            },
                        },
                    },
                ],
            },
        )
        cluster_id = response.get("JobFlowId")
        console_log(f"Created EMR Cluster: {cluster_id}")
        return cluster_id


class EMREC2:
    def __init__(
        self,
        cluster_id: str,
        deployment_package: DeploymentPackage,
        job_role: Optional[str] = None,
        region: str = "",
    ) -> None:
        self.cluster_id = cluster_id
        self.dp = deployment_package
        self.job_role = job_role
        self.client = boto3.client("emr")
        self.s3_client = boto3.client("s3")

    def run_job(
        self,
        job_name: str,
        job_args: Optional[List[str]] = None,
        spark_submit_opts: Optional[str] = None,
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

        if spark_submit_opts:
            spark_submit_params = f"{spark_submit_params} {spark_submit_opts}".strip()

        # Escape job args if they're provided
        if job_args:
            job_args = [shlex.quote(arg) for arg in job_args]

        # show_logs is only compatible with client mode
        # --conf spark.archives is only compatible with cluster mode
        # So if we have both, we have to throw an error
        # See https://issues.apache.org/jira/browse/SPARK-36088
        if show_logs and ("--conf spark.archives" in spark_submit_params or "--archives" in spark_submit_params):
            raise RuntimeError(
                "--show-stdout is not compatible with projects that make use of "
                + "dependencies.\nPlease 👍 this GitHub issue to voice your support: "
                + "https://github.com/awslabs/amazon-emr-cli/issues/12"
            )

        # define params for emr.add_job_flow_steps
        add_job_flow_steps_params = {
            "JobFlowId": self.cluster_id,
            "Steps": [
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
                        + [self.dp.entrypoint_uri()]
                        + (job_args or []),
                    },
                }
            ],
        }

        # conditionally add ExecutionRoleArn to add_job_flow_steps if a runtime role is requested for this step
        # https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-steps-runtime-roles.html
        if self.job_role:
            add_job_flow_steps_params["ExecutionRoleArn"] = self.job_role

        try:
            response = self.client.add_job_flow_steps(**add_job_flow_steps_params)
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
                sys.exit(1)

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
                "MaxAttempts": int(timeout_secs / LOG_WAITER_DELAY_SEC),
            },
        )
        return object_name
