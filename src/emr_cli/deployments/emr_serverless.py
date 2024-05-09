import abc
import json
import os
import sys
import zipfile
from os.path import join
from time import sleep
from typing import List, Optional

import boto3
from emr_cli.deployments import SparkParams
from emr_cli.utils import console_log, find_files, mkdir, print_s3_gz


class DeploymentPackage(metaclass=abc.ABCMeta):
    def __init__(self, entry_point_path: str = "entrypoint.py", s3_target_uri: str = "") -> None:
        self.entry_point_path = entry_point_path
        self.dist_dir = "dist"

        # We might not populate this until we actually deploy
        self.s3_uri_base = s3_target_uri

    def spark_submit_parameters(self) -> SparkParams:
        """
        Returns any additional arguments necessary for spark-submit
        """
        return SparkParams()

    def entrypoint_uri(self) -> str:
        """
        Returns the full S3 URI to the entrypoint file, e.g. s3://bucket/path/somecode.py
        """
        if self.s3_uri_base is None:
            raise Exception("S3 URI has not been set, aborting")
        return os.path.join(self.s3_uri_base, self.entry_point_path)

    def _zip_local_pyfiles(self):
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


class Bootstrap:
    # Maybe add some UUIDs to these?
    DEFAULT_S3_POLICY_NAME = "emr-cli-S3Access"
    DEFAULT_GLUE_POLICY_NAME = "emr-cli-GlueAccess"

    def __init__(self, code_bucket: str, log_bucket: str, job_role_name: str):
        self.code_bucket = code_bucket
        self.log_bucket = log_bucket or code_bucket
        self.job_role_name = job_role_name
        self.s3_client = boto3.client("s3")
        self.iam_client = boto3.client("iam")
        self.emrs_client = boto3.client("emr-serverless")

    def create_environment(self):
        self._create_s3_buckets()
        job_role_arn = self._create_job_role()
        app_id = self._create_application()
        return {
            "application_id": app_id,
            "job_role_arn": job_role_arn,
            "code_bucket": self.code_bucket,
            "log_bucket": self.log_bucket,
        }

    def print_destroy_commands(self, application_id: str):
        # fmt: off
        for bucket in set([self.log_bucket, self.code_bucket]):
            print(f"aws s3 rm s3://{bucket} --recursive")
            print(f"aws s3api delete-bucket --bucket {bucket}")
        for policy in self.iam_client.list_attached_role_policies(RoleName=self.job_role_name).get('AttachedPolicies'):  # noqa E501
            arn = policy.get('PolicyArn')
            print(f"aws iam detach-role-policy --role-name {self.job_role_name} --policy-arn {arn}")  # noqa E501
            print(f"aws iam delete-policy --policy-arn {arn}")  # noqa E501
        print(f"aws iam delete-role --role-name {self.job_role_name}")
        print(f"aws emr-serverless stop-application --application-id {application_id}")
        print(f"aws emr-serverless delete-application --application-id {application_id}")  # noqa E501
        # fmt: on

    def _create_s3_buckets(self):
        """
        Creates both the source and log buckets if they don't already exist.
        """
        for bucket_name in set([self.code_bucket, self.log_bucket]):
            self.s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={
                    "LocationConstraint": self.s3_client.meta.region_name  # type: ignore
                },
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

    def _create_job_role(self):
        # First create a role that can be assumed by EMR Serverless jobs
        response = self.iam_client.create_role(
            RoleName=self.job_role_name,
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "emr-serverless.amazonaws.com"},
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

    def _create_application(self):
        """
        Create a simple Spark EMR Serverless application with a default (but minimal)
        pre-initialized capacity.

        This application is only intended for demo purposes only. To customize the
        application or create an application for production, use the AWS CLI or other
        Infrastructure as Code services like Terraform, CDK, or CloudFormation.
        """
        response = self.emrs_client.create_application(
            name="emr-cli-demo",
            releaseLabel="emr-6.9.0",
            type="SPARK",
        )
        app_id = response.get("applicationId")
        console_log(f"Created EMR Serverless application: {app_id}")
        self.emrs_client.start_application(applicationId=app_id)
        return app_id


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
        self.s3_client = boto3.client("s3")
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
        show_logs: bool = False,
        s3_logs_uri: Optional[str] = None,
        timeout: Optional[int] = None,
    ):
        if show_logs and not s3_logs_uri:
            raise RuntimeError("--show-stdout requires --s3-logs-uri to be set.")

        jobDriver = {
            "sparkSubmit": {
                "entryPoint": self.dp.entrypoint_uri(),
            }
        }
        spark_submit_parameters = self.dp.spark_submit_parameters().params_for("emr_serverless")

        if spark_submit_opts:
            spark_submit_parameters = f"{spark_submit_parameters} {spark_submit_opts}".strip()

        if spark_submit_parameters:
            jobDriver["sparkSubmit"]["sparkSubmitParameters"] = spark_submit_parameters

        if job_args:
            jobDriver["sparkSubmit"]["entryPointArguments"] = job_args  # type: ignore

        config_overrides = {}
        if s3_logs_uri:
            config_overrides = {"monitoringConfiguration": {"s3MonitoringConfiguration": {"logUri": s3_logs_uri}}}

        if timeout is None:
            timeout = 12 * 60  # set to AWS default value (12 hours in minutes)

        response = self.client.start_job_run(
            applicationId=self.application_id,
            executionRoleArn=self.job_role,
            name=job_name,
            jobDriver=jobDriver,
            configurationOverrides=config_overrides,
            executionTimeoutMinutes=timeout,
        )
        job_run_id = response.get("jobRunId")

        console_log(f"Job submitted to EMR Serverless (Job Run ID: {job_run_id})")
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
                "SUCCESS",
                "FAILED",
                "CANCELLING",
                "CANCELLED",
            ]
            sleep(2)

        if show_logs:
            console_log(f"stdout for {job_run_id}\n{'-'*38}")
            log_location = join(
                f"{s3_logs_uri}",
                "applications",
                self.application_id,
                "jobs",
                job_run_id,
                "SPARK_DRIVER",
                "stdout.gz",
            )
            print_s3_gz(self.s3_client, log_location)

        if jr_response.get("state") != "SUCCESS":
            console_log(f"EMR Serverless job failed: {jr_response.get('stateDetails')}")
            sys.exit(1)
        console_log("Job completed successfully!")

        return job_run_id

    def get_job_run(self, job_run_id: str) -> dict:
        response = self.client.get_job_run(applicationId=self.application_id, jobRunId=job_run_id)
        return response.get("jobRun")
