from typing import Dict, Optional


class SparkParams:
    """
    SparkParams allows deployment packages to specify different sets of
    Spark `--conf` parameters based on the environment being deployed to.
    """

    SUPPORTED_ENVIRONMENTS = ["emr_serverless", "emr_ec2", "emr_eks"]

    def __init__(
        self,
        common_params: Optional[Dict[str, str]] = None,
        emr_serverless_params: Optional[Dict[str, str]] = None,
        emr_ec2_params: Optional[Dict[str, str]] = None,
        emr_eks_params: Optional[Dict[str, str]] = None,
    ) -> None:
        self._common = common_params or {}
        self._environment_params = {
            "emr_serverless": emr_serverless_params or {},
            "emr_ec2": emr_ec2_params or {},
            "emr_eks": emr_eks_params or {},
        }

    def params_for(self, deployment_type: str) -> str:
        """
        Return a set of string spark-submit parameters for the provided deployment type.
        """
        if deployment_type not in self.SUPPORTED_ENVIRONMENTS:
            raise ValueError(f"{deployment_type} environment is not supported.")

        conf_items = {}

        for k, v in self._common.items():
            conf_items[k] = v

        for k, v in self._environment_params[deployment_type].items():
            conf_items[k] = v

        return " ".join([f"--conf {k}={v}" for k, v in conf_items.items()])
