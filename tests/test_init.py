from emr_cli.packaging.python_poetry_project import PythonPoetryProject
from emr_cli.packaging.python_project import PythonProject


class TestInit:
    def test_default_init(self, tmp_path):
        p = PythonProject()

        p.initialize(tmp_path)
        assert (tmp_path / "pyproject.toml").exists()
        assert (tmp_path / "entrypoint.py").exists()
        assert (tmp_path / "jobs" / "extreme_weather.py").exists()
        assert not (tmp_path / "README.md").exists()

    def test_poetry_init(self, tmp_path):
        p = PythonPoetryProject()

        p.initialize(tmp_path)
        assert (tmp_path / "entrypoint.py").exists()
        assert (tmp_path / "pyproject.toml").exists()
        assert (tmp_path / "README.md").exists()
