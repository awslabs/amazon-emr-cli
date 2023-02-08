import click

from emr_cli.packaging.detector import ProjectDetector
from .deployments.emr_serverless import EMRServerless
from .packaging.python_project import PythonProject


@click.group()
@click.pass_context
def cli(ctx):
    ctx.obj = ProjectDetector().detect()


@click.command()
@click.option("--dockerfile", default=False, is_flag=True, help="Only create a sample Dockerfile for packaging Python dependencies")
def init(dockerfile):
    if dockerfile:
        click.echo("Creating sample Dockerfile...")
        PythonProject().copy_single_file('Dockerfile')
    else:
        click.echo("Initializing project")
        PythonProject().initialize()


@click.command()
@click.option(
    "--entry-point",
    type=click.Path(exists=True, dir_okay=False, allow_dash=False),
    help="Entrypoint file",
    required=True,
)
@click.pass_obj
def package(project, entry_point):
    p = project(entry_point)
    p.build()


@click.command()
@click.option(
    "--entry-point",
    type=click.Path(exists=True, dir_okay=False, allow_dash=False),
    help="PySpark file to deploy",
)
@click.option("--s3-code-uri", help="Where to copy code artifacts to")
@click.pass_obj
def deploy(project, entry_point, s3_code_uri):
    # Copy the source to S3
    p = project(entry_point)
    p.deploy(s3_code_uri)


@click.command()
@click.option("--application-id", help="EMR Serverless Application ID", required=True)
@click.option("--entry-point", help="Python or Jar file for the main entrypoint")
@click.option("--job-role", help="IAM Role ARN to use for the job execution")
@click.option("--wait", default=False, is_flag=True, help="Wait for job to finish")
@click.option("--s3-code-uri", help="Where to copy code artifacts to")
@click.option("--job-name", help="The name of the job", default="emr-cli job")
@click.option(
    "--job-args",
    help="Comma-delimited string of arguments to be passed to Spark job",
    default=None,
)
@click.option(
    "--build",
    help="Do not package and deploy the job assets",
    default=False,
    is_flag=True,
)
@click.pass_obj
def run(
    project, 
    application_id, entry_point, job_role, wait, s3_code_uri, job_name, job_args, build
):
    # We require entry-point and s3-code-uri
    if entry_point is None or s3_code_uri is None:
        raise click.BadArgumentUsage(
            "--entry-point and --s3-code-uri are required if --build is used."
        )
    p = project(entry_point, s3_code_uri)

    if build:
        p.build()
        p.deploy(s3_code_uri)

    if application_id is not None:
        # We require entry-point and job-role
        if entry_point is None or job_role is None:
            raise click.BadArgumentUsage(
                "--entry-point and --job-role are required if --application-id is used."
            )

        if job_args:
            job_args = job_args.split(",")
        emrs = EMRServerless(application_id, job_role, p)
        emrs.run_job(job_name, job_args, wait)


cli.add_command(package)
cli.add_command(deploy)
cli.add_command(run)
cli.add_command(init)


if __name__ == "__main__":
    cli()
