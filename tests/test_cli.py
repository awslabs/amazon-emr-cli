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
    
    def test_resource_validation(self):
        runner = CliRunner()
        result = runner.invoke(cli, ['run'])
        assert result.exit_code == 2
        assert 'Error: One of' in result.output
        assert "must be specified" in result.output

        result = runner.invoke(cli, ['run', '--application-id', '1234', '--cluster-id', '567'])
        assert result.exit_code == 2
        assert 'Error: Only one of' in result.output
        assert "can be specified" in result.output

        for arg in ['--application-id', '--cluster-id', '--virtual-cluster-id']:
            result = runner.invoke(cli, ['run', arg, '1234'])
            assert result.exit_code == 2
            assert 'Error: --entry-point' in result.output
