from typing import Dict, Optional


class SparkParams:
    def __init__(
        self,
        common_params: Optional[Dict[str, str]] = None,
        emr_serverless_params: Optional[Dict[str, str]] = None,
        emr_ec2_params: Optional[Dict[str, str]] = None,
    ) -> None:
        self._common = common_params or {}
        self._emr_serverless = emr_serverless_params or {}
        self._emr_ec2 = emr_ec2_params or {}

    def params_for(self, deployment_type: str) -> str:
        conf_items = {}

        for k, v in self._common.items():
            conf_items[k] = v

        if deployment_type == "emr_serverless":
            for k, v in self._emr_serverless.items():
                conf_items[k] = v

        return " ".join([f"--conf {k}={v}" for k, v in conf_items.items()])
