from emr_cli.packaging.detector import ProjectDetector
from emr_cli.packaging.python_files_project import PythonFilesProject
from emr_cli.packaging.python_poetry_project import PythonPoetryProject
from emr_cli.packaging.python_project import PythonProject
from emr_cli.packaging.simple_project import SimpleProject


class TestDetector:
    def test_single_py_file(self, fs):
        fs.create_file("main.py")
        obj = ProjectDetector().detect()
        assert obj == SimpleProject

    def test_multi_py_file(self, fs):
        fs.create_file("main.py")
        fs.create_file("lib/file1.py")
        fs.create_file("lib/file2.py")
        obj = ProjectDetector().detect()
        assert obj == PythonFilesProject

    def test_poetry_project(self, fs):
        fs.create_file("poetry.lock")
        obj = ProjectDetector().detect()
        assert obj == PythonPoetryProject

    def test_dependency_project(self, fs):
        fs.create_file("main.py")
        fs.create_file("pyproject.toml")
        fs.create_file("lib/file1.py")
        fs.create_file("lib/file2.py")
        obj = ProjectDetector().detect()
        assert obj == PythonProject
