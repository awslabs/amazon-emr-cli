from emr_cli.packaging.python_poetry_project import PythonPoetryProject
from emr_cli.packaging.python_project import PythonProject


class TestInit:
    def test_default_init(self, tmp_path):
        p = PythonProject()
        target_path = tmp_path / "python_proj"
        p.initialize(target_path)
        assert (target_path / "pyproject.toml").exists()
        assert (target_path / "entrypoint.py").exists()
        assert (target_path / "jobs" / "extreme_weather.py").exists()
        assert not (target_path / "README.md").exists()

    def test_poetry_init(self, tmp_path):
        p = PythonPoetryProject()
        target_path = tmp_path / "python_poetry_proj"
        p.initialize(target_path)
        assert (target_path / "entrypoint.py").exists()
        assert (target_path / "pyproject.toml").exists()
        assert (target_path / "README.md").exists()

    def test_create_in_existing_folder(self, tmp_path):
        pass
