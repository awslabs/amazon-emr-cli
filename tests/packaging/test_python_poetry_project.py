from emr_cli.deployments import SparkParams
from emr_cli.packaging.python_poetry_project import PythonPoetryProject


class TestPythonFilesProject:
    def test_spark_submit(self, fs):
        fs.create_file("main.py")
        fs.create_file("lib/file1.py")
        fs.create_file("lib/file2.py")
        ppp = PythonPoetryProject("main.py")
        sp = ppp.spark_submit_parameters()
        assert type(sp) == SparkParams
        assert "spark.archives" in sp.params_for("emr_serverless")
        assert "spark.emr-serverless.driverEnv" in sp.params_for("emr_serverless")
