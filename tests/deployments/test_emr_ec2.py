import unittest
from unittest.mock import MagicMock

from emr_cli.deployments.emr_ec2 import EMREC2
from emr_cli.deployments.emr_serverless import DeploymentPackage

CLUSTER_ID = "j-11111111"


class TestEMREC2(unittest.TestCase):
    def setUp(self):
        self.obj = EMREC2(CLUSTER_ID, DeploymentPackage())

    def test_fetch_log_location_success(self):
        self.obj.client.describe_cluster = MagicMock(
            return_value={"Cluster": {"LogUri": "s3n://example-bucket/logs/"}}
        )
        self.assertEqual(self.obj._fetch_log_location(), "s3://example-bucket/logs/")

    def test_fetch_log_location_no_loguri(self):
        self.obj.client.describe_cluster = MagicMock(return_value={"Cluster": {}})
        # Ensure that a RuntimeError is raised
        with self.assertRaises(RuntimeError):
            self.obj._fetch_log_location()

    def test_fetch_log_location_loguri_none(self):
        self.obj.client.describe_cluster = MagicMock(
            return_value={"Cluster": {"LogUri": None}}
        )
        # Ensure that a RuntimeError is raised
        with self.assertRaises(RuntimeError):
            self.obj._fetch_log_location()

    def test_fetch_log_location_replace_s3n_with_s3(self):
        self.obj.client.describe_cluster = MagicMock(
            return_value={"Cluster": {"LogUri": "s3n://example-bucket/logs/"}}
        )
        # Ensure that "s3n:" is replaced with "s3:" in the returned S3 location
        self.assertEqual(self.obj._fetch_log_location(), "s3://example-bucket/logs/")
