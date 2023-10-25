from click.testing import CliRunner

from emr_cli.emr_cli import cli

class TestCli:
    def test_version(self):
        runner = CliRunner()
        result = runner.invoke(cli, ['status'])
        assert result.exit_code == 0
        assert 'EMR CLI version:' in result.output
    
    def test_project_detection(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            with open('main.py', 'w') as f:
                f.write('print("Hello World")')
            
            result = runner.invoke(cli, ['status'])
            assert result.exit_code == 0
            assert 'Project type:\t\tSimpleProject' in result.output