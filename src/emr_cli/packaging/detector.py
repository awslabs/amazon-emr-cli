import os
from typing import Optional
from emr_cli.deployments.emr_serverless import DeploymentPackage

from emr_cli.packaging.python_files_project import PythonFilesProject
from emr_cli.packaging.python_poetry_project import PythonPoetryProject
from emr_cli.packaging.python_project import PythonProject
from emr_cli.packaging.simple_project import SimpleProject
from emr_cli.utils import find_files


class ProjectDetector:
    """
    Detects the type of package used for Spark deployment.
    - Single PySpark file
    - setuptools-based project
    - poetry project
    - requirements.txt
    """

    PROJECT_TYPE_MAPPINGS = {
        "single-file": SimpleProject,
        "python": PythonProject,
        "poetry": PythonPoetryProject,
    }

    def detect(self, project_type: Optional[str] = None) -> DeploymentPackage.__class__:
        if project_type:
            if project_type not in self.PROJECT_TYPE_MAPPINGS:
                raise ValueError(f"Unknown project type {project_type}")
            return self.PROJECT_TYPE_MAPPINGS.get(project_type) # type: ignore

        # We default to a single file project - if the user has just a .py or .jar
        project = SimpleProject

        # If there are multiple .py files, we escalate to a PythonProject
        if len(find_files(os.getcwd(), [".venv"], ".py")) > 1:
            project = PythonFilesProject

        # If we have a pyproject.toml or setup.py, we have a python project
        if find_files(os.getcwd(), [".venv"], "pyproject.toml") or find_files(
            os.getcwd(), [".venv"], "setup.py"
        ):
            project = PythonProject

        # If we have a poetry.lock, it's a poetry project
        if find_files(os.getcwd(), [".venv"], "poetry.lock"):
            project = PythonPoetryProject

        return project
