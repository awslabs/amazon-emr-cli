from pathlib import Path

import yaml

from emr_cli.utils import console_log

DEFAULT_CONFIG_PATH = ".emr/config.yaml"


class ConfigReader:
    @classmethod
    def read(cls):
        config = {}
        # Look for a config file - if we don't find one, that's fine. :)
        p = Path(DEFAULT_CONFIG_PATH)
        if not p.is_file():
            return config

        with p.open() as infile:
            try:
                config = yaml.safe_load(infile)
                return config
            except yaml.YAMLError as exc:
                console_log(f"There was an error parsing the config file: {exc}")
                return config


class ConfigWriter:
    @classmethod
    def write(cls, config):
        """
        Write the passed config, overwriting any existing config.
        """
        p = Path(DEFAULT_CONFIG_PATH)

        p.parent.mkdir(parents=True, exist_ok=True)

        with p.open("w") as outfile:
            outfile.write(yaml.dump(config))
