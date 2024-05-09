try:
    from importlib.metadata import version
except ModuleNotFoundError:
    # Python 3.7 compatibility
    # https://github.com/python/importlib_metadata#compatibility-with-python-3.7
    from importlib_metadata import version

import click
from emr_cli.config import DEFAULT_CONFIG_PATH, ConfigReader, ConfigWriter
from emr_cli.deployments.emr_ec2 import EMREC2
from emr_cli.deployments.emr_ec2 import Bootstrap as BootstrapEMRonEC2
from emr_cli.deployments.emr_eks import EMREKS
from emr_cli.packaging.detector import ProjectDetector
from emr_cli.utils import console_log

from .deployments.emr_serverless import Bootstrap as BootstrapEMRServerless
from .deployments.emr_serverless import EMRServerless
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
    if ctx.default_map:
        console_log(f"Using config file: {DEFAULT_CONFIG_PATH}")


@click.command()
@click.pass_obj
def status(project):
    console_log("")
    print(f"Project type:\t\t{ project.__name__}")
    print(f"EMR CLI version:\t{version('emr-cli')}")


@click.command()
@click.option(
    "--target",
    type=click.Choice(["emr-serverless", "emr-ec2"]),
    help="Bootstrap a brand new environment.",
)
@click.option("--code-bucket", help="Bucket where source code will be uploaded", required=True)
@click.option("--logs-bucket", help="Bucket where logs will be uploaded")
@click.option(
    "--instance-profile-name",
    help="""
The name of the IAM role to be created for your EMR on EC2 instances.
""",
    required=False,
)
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
def bootstrap(target, code_bucket, logs_bucket, instance_profile_name, job_role_name, destroy):
    """
    Bootstrap an EMR Serverless environment.

    Includes creating an S3 bucket, tightly-scoped job roles,
    EMR Serverless application, and emr cli configuration file.
    """
    # EMR on EC2 additionally needs an instance profile role
    if target == "emr-ec2" and instance_profile_name is None:
        raise click.BadArgumentUsage("EMR on EC2 clusters require --instance-profile-name to be set.")

    if target == "emr-serverless":
        b = BootstrapEMRServerless(code_bucket, logs_bucket, job_role_name)
    else:
        b = BootstrapEMRonEC2(code_bucket, logs_bucket, instance_profile_name, job_role_name)

    resource_id = "application_id" if target == "emr-serverless" else "cluster_id"
    if destroy:
        c = ConfigReader.read()
        b.print_destroy_commands(c.get("run", {}).get(resource_id, None))
        exit(0)

    # For EMR Serverless, we need to create an S3 bucket, a job role, and an Application
    config = b.create_environment()

    # The resulting config is relevant for the "run" command
    run_config = {
        "run": {
            resource_id: config.get(resource_id),
            "job_role": config.get("job_role_arn"),
            "s3_code_uri": f"s3://{config.get('code_bucket')}/code/pyspark/",
            "s3_logs_uri": f"s3://{config.get('log_bucket')}/logs/pyspark/",
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
@click.option("--virtual-cluster-id", help="EMR on EKS Virtual Cluster ID")
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
@click.option(
    "--save-config",
    help="Update the config file with the provided options",
    is_flag=True,
)
@click.option(
    "--emr-eks-release-label", help="EMR on EKS release label (emr-6.15.0) - defaults to latest release", default=None
)
@click.option(
    "--emr-serverless-timeout",
    help="EMR Serverless job timeout in minutes - defaults to 12 hours",
    default=None,
    type=int
)
@click.pass_obj
@click.pass_context
def run(
    ctx,
    project,
    application_id,
    cluster_id,
    virtual_cluster_id,
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
    save_config,
    emr_eks_release_label,
    emr_serverless_timeout,
):
    """
    Run a project on EMR, optionally build and deploy
    """
    resource_ids = [cluster_id, application_id, virtual_cluster_id]

    # A resource ID must be specified
    if not any(resource_ids):
        raise click.BadArgumentUsage(
            "One of --application-id, --cluster-id, or --virtual-cluster-id must be specified."
        )

    # Only one resource ID can be specified
    if resource_ids.count(None) != (len(resource_ids) - 1):
        raise click.BadArgumentUsage(
            "Only one of --application-id, --cluster-id, or --virtual-cluster-id can be specified"
        )

    # We require entry-point and s3-code-uri
    if entry_point is None or s3_code_uri is None:
        raise click.BadArgumentUsage("--entry-point and --s3-code-uri are required.")
    p = project(entry_point, s3_code_uri)

    # Do a brief validation of the EMR on EKS release label
    if emr_eks_release_label:
        if not virtual_cluster_id:
            raise click.BadArgumentUsage("--emr-eks-release-label can only be used with --virtual-cluster-id")
        elif not emr_eks_release_label.startswith("emr-"):
            raise click.BadArgumentUsage(f"--emr-eks-release-label must start with 'emr-', provided '{emr_eks_release_label}'")

    # If the user passes --save-config, update our stored config file
    if save_config:
        run_config = {"run": ctx.__dict__.get("params")}
        del run_config["run"]["save_config"]
        ConfigWriter.write(run_config)
        console_log(f"Config file saved to {DEFAULT_CONFIG_PATH}. Use `emr run` to re-use your configuration.")  # noqa: E501

    if build:
        p.build()
        p.deploy(s3_code_uri)

    if any([application_id, virtual_cluster_id]):
        # We require entry-point and job-role
        if entry_point is None or job_role is None:
            raise click.BadArgumentUsage(
                "--entry-point and --job-role are required if --application-id or --virtual-cluster-id is used."
            )

    if emr_serverless_timeout < 0:
        raise click.BadArgumentUsage("--emr-serverless-timeout must be greater than or equal to 0.")

    # application_id indicates EMR Serverless job
    if application_id is not None:
        if job_args:
            job_args = job_args.split(",")
        emrs = EMRServerless(application_id, job_role, p)
        emrs.run_job(job_name, job_args, spark_submit_opts, wait, show_stdout, s3_logs_uri, emr_serverless_timeout)

    # cluster_id indicates EMR on EC2 job
    if cluster_id is not None:
        if job_args:
            job_args = job_args.split(",")
        emr = EMREC2(cluster_id, p, job_role)
        emr.run_job(job_name, job_args, spark_submit_opts, wait, show_stdout)

    # virtual_cluster_id is EMR on EKS
    if virtual_cluster_id is not None:
        if job_args:
            job_args = job_args.split(",")
        emreks = EMREKS(virtual_cluster_id, job_role, p)
        emreks.run_job(job_name, job_args, spark_submit_opts, wait, show_stdout, s3_logs_uri, emr_eks_release_label)


cli.add_command(package)
cli.add_command(deploy)
cli.add_command(run)
cli.add_command(init)
cli.add_command(bootstrap)
cli.add_command(status)

if __name__ == "__main__":
    cli()  # type: ignore
