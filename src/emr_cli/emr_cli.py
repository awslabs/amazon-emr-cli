from sys import is_finalizing
import click
from emr_cli.config import ConfigReader, ConfigWriter
from emr_cli.deployments.emr_ec2 import EMREC2
from emr_cli.packaging.detector import ProjectDetector
from emr_cli.utils import console_log

from .deployments.emr_serverless import Bootstrap, EMRServerless
from .packaging.python_project import PythonProject


@click.group()
@click.pass_context
def cli(ctx):
    """
    Package, deploy, and run PySpark projects on EMR.
    """
    # If we want the user to be able to force a project type, check out click.Choice
    ctx.obj = ProjectDetector().detect()

    # If a config file exists, set those as defaults for all other options
    ctx.default_map = ConfigReader.read()


@click.command()
@click.pass_obj
def status(project):
    console_log("")
    print("Project type:\t", project.__name__)


@click.command()
@click.option(
    "--target",
    type=click.Choice(["emr-serverless"]),
    help="Bootstrap a brand new environment.",
)
@click.option(
    "--code-bucket", help="Bucket where source code will be uploaded", required=True
)
@click.option("--logs-bucket", help="Bucket where logs will be uploaded")
@click.option(
    "--job-role-name",
    help="""
The name of the IAM role to be created for your EMR Serverless jobs.
This role has access to read and write to the source code and logs buckets,
and access to read and create tables in the Glue Data Catalog.""",
    required=True,
)
@click.option(
    "--destroy",
    default=False,
    is_flag=True,
    help="Prints the commands necessary to destroy the created environment.",
)
def bootstrap(target, code_bucket, logs_bucket, job_role_name, destroy):
    """
    Bootstrap an EMR Serverless environment.

    Includes creating an S3 bucket, tightly-scoped job roles,
    EMR Serverless application, and emr cli configuration file.
    """
    if destroy:
        c = ConfigReader.read()
        b = Bootstrap(code_bucket, logs_bucket, job_role_name)
        b.print_destroy_commands(c.get("run", {}).get("application_id", None))
        exit(0)

    # For EMR Serverless, we need to create an S3 bucket, a job role, and an Application
    b = Bootstrap(code_bucket, logs_bucket, job_role_name)
    config = b.create_environment()

    # The resulting config is relevant for the "run" command
    run_config = {
        "run": {
            "application_id": config.get("application_id"),
            "job_role": config.get("job_role_arn"),
            "s3_code_uri": f"s3://{config.get('code_bucket')}/code/pyspark/",
        }
    }
    ConfigWriter.write(run_config)


@click.command()
@click.argument("path")
@click.option(
    "--dockerfile",
    default=False,
    is_flag=True,
    help="Only create a sample Dockerfile for packaging Python dependencies",
)
@click.option(
    "--project-type",
    type=click.Choice(["python", "poetry"]),
    help="The type of project to create.",
    default="python",
)
def init(path, dockerfile, project_type):
    """
    Initialize a local PySpark project.
    """
    if dockerfile:
        click.echo("Creating sample Dockerfile...")
        PythonProject().copy_single_file("Dockerfile")
    else:
        kls = ProjectDetector().detect(project_type)
        kls().initialize(path)


@click.command()
@click.option(
    "--entry-point",
    type=click.Path(exists=True, dir_okay=False, allow_dash=False),
    help="Entrypoint file",
    required=True,
)
@click.pass_obj
def package(project, entry_point):
    """
    Package a project and dependencies into dist/
    """
    p = project(entry_point)
    p.build()


@click.command()
@click.option(
    "--entry-point",
    type=click.Path(exists=True, dir_okay=False, allow_dash=False),
    help="PySpark file to deploy",
    required=True,
)
@click.option(
    "--s3-code-uri",
    help="Where to copy code artifacts to",
    required=True,
)
@click.pass_obj
def deploy(project, entry_point, s3_code_uri):
    """
    Copy a local project to S3.
    """
    p = project(entry_point)
    p.deploy(s3_code_uri)


@click.command()
@click.option("--application-id", help="EMR Serverless Application ID")
@click.option("--cluster-id", help="EMR on EC2 Cluster ID")
@click.option(
    "--entry-point",
    type=click.Path(exists=True, dir_okay=False, allow_dash=False),
    help="Python or Jar file for the main entrypoint",
)
@click.option("--job-role", help="IAM Role ARN to use for the job execution")
@click.option("--wait", default=False, is_flag=True, help="Wait for job to finish")
@click.option("--s3-code-uri", help="Where to copy/run code artifacts to/from")
@click.option("--s3-logs-uri", help="Where to send EMR Serverless logs to")
@click.option("--job-name", help="The name of the job", default="emr-cli job")
@click.option(
    "--job-args",
    help="Comma-delimited string of arguments to be passed to Spark job",
    default=None,
)
@click.option(
    "--spark-submit-opts",
    help="String of spark-submit options",
    default=None,
)
@click.option(
    "--build",
    help="Package and deploy job artifacts",
    default=False,
    is_flag=True,
)
@click.option(
    "--show-stdout",
    help="Show the stdout of the job after it's finished",
    default=False,
    is_flag=True,
)
@click.pass_obj
def run(
    project,
    application_id,
    cluster_id,
    entry_point,
    job_role,
    wait,
    s3_code_uri,
    s3_logs_uri,
    job_name,
    job_args,
    spark_submit_opts,
    build,
    show_stdout,
):
    """
    Run a project on EMR, optionally build and deploy
    """
    # Either a cluster or applciation ID must be specified
    if cluster_id is None and application_id is None:
        raise click.BadArgumentUsage(
            "Either --application-id or --cluster-id must be specified."
        )
        
    # We require entry-point and s3-code-uri
    if entry_point is None or s3_code_uri is None:
        raise click.BadArgumentUsage(
            "--entry-point and --s3-code-uri are required if --build is used."
        )
    p = project(entry_point, s3_code_uri)

    if build:
        p.build()
        p.deploy(s3_code_uri)

    # application_id indicates EMR Serverless job
    if application_id is not None:
        # We require entry-point and job-role
        if entry_point is None or job_role is None:
            raise click.BadArgumentUsage(
                "--entry-point and --job-role are required if --application-id is used."
            )

        if job_args:
            job_args = job_args.split(",")
        emrs = EMRServerless(application_id, job_role, p)
        emrs.run_job(job_name, job_args, spark_submit_opts, wait, show_stdout, s3_logs_uri)

    # cluster_id indicates EMR on EC2 job
    if cluster_id is not None:
        if job_args:
            job_args = job_args.split(",")
        emr = EMREC2(cluster_id, p)
        emr.run_job(job_name, job_args, wait, show_stdout)


cli.add_command(package)
cli.add_command(deploy)
cli.add_command(run)
cli.add_command(init)
cli.add_command(bootstrap)
cli.add_command(status)

if __name__ == "__main__":
    cli()
