from pathlib import Path
from emr_cli.deployments import SparkParams

from emr_cli.packaging.python_files_project import PythonFilesProject


class TestPythonFilesProject:
    def test_build(self, fs):
        fs.create_file("main.py")
        fs.create_file("lib/file1.py")
        fs.create_file("lib/file2.py")
        pfp = PythonFilesProject("main.py")
        pfp.build()
        assert Path("dist/pyfiles.zip").exists()
    
    def test_spark_submit(self, fs):
        fs.create_file("main.py")
        fs.create_file("lib/file1.py")
        fs.create_file("lib/file2.py")
        pfp = PythonFilesProject("main.py")
        sp = pfp.spark_submit_parameters()
        assert type(sp) == SparkParams
        assert sp.params_for("emr_serverless").startswith("--conf spark.submit.pyFiles=")
