# EMR CLI

So we're all working on data pipelines every day, but wouldn't be nice to just hit a button and have our code automatically deployed to staging or test accounts? I thought so, too, thats why I created the EMR CLI (`emr`) that can help you package and deploy your EMR jobs so you don't have to.

The EMR CLI supports a wide variety of configuration options to adapt to _your_ data pipeline, not the other way around.

1. Packaging - Ensure a consistent approach to packaging your production Spark jobs.
2. Deployment - Easily deploy your Spark jobs across multiple EMR environments or deployment frameworks like EC2, EKS, and Serverless.
3. CI/CD - Easily test each iteration of your code without resorting to messy shell scripts. :)

The initial use cases are:

1. Consistent packaging for PySpark projects.
2. Use in CI/CD pipelines for packaging, deployment of artifacts, and integration testing.

> **Warning**: This tool is still under active development, so commands may change until a stable 1.0 release is made.

## Quick Start

You can use the EMR CLI to take a project from nothing to running in EMR Serverless is 2 steps.

First, let's install the `emr` command.

```bash
python3 -m pip install -U emr-cli
```

> **Note** This tutorial assumes you have already [setup EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/setting-up.html) and have an EMR Serverless application, job role, and S3 bucket you can use. If not, you can use the `emr bootstrap` command.

1. Create a sample project

```bash
emr init scratch
```

> 📔 Tip: Use `--project-type poetry` to create a [Poetry](https://python-poetry.org/) project!

You should now have a sample PySpark project in your scratch directory.

```
scratch
├── Dockerfile
├── entrypoint.py
├── jobs
│   └── extreme_weather.py
└── pyproject.toml

1 directory, 4 files
```

2. Now deploy and run on an EMR Serverless application!

```bash
emr run \
    --entry-point entrypoint.py \
    --application-id ${APPLICATION_ID} \
    --job-role ${JOB_ROLE_ARN} \
    --s3-code-uri  s3://${S3_BUCKET}/tmp/emr-cli-demo/ \
    --s3-logs-uri  s3://${S3_BUCKET}/logs/emr-cli-demo/ \
    --build \
    --show-stdout
```

This command performs the following actions:

- Packages your project dependencies into a Python virtual environment
- Uploads the Spark entrypoint and packaged dependencies to S3
- Starts an EMR Serverless job
- Waits for the job to run to completion and shows the `stdout` of the Spark driver when finished!

And you're done. Feel free to modify the project to experiment with different things. You can simply re-run the command above to re-package and re-deploy your job.

## EMR CLI Sub-commands

The EMR CLI has several subcommands that you can see by running `emr --help`

```
Commands:
  bootstrap  Bootstrap an EMR Serverless environment.
  deploy     Copy a local project to S3.
  init       Initialize a local PySpark project.
  package    Package a project and dependencies into dist/
  run        Run a project on EMR, optionally build and deploy
  status
```

### bootstrap

`emr bootstrap` allows you to create a sample EMR Serverless or EMR on EC2 environment for testing. It assumes you have admin access and creates various resources for you using AWS APIs.

#### EMR Serverless

To create a bootstrap EMR Serverless environment, using the following command:

```shell
emr bootstrap \
    --target emr-serverless \
    --code-bucket <your_unique_new_bucket_name> \
    --job-role-name <your_unique_emr_serverless_job_role_name>
```

When you do this, the CLI creates a new EMR CLI config file at `.emr/config.yaml` that will set default locations for your `emr run` command.

### init

The `init` command creates a new `pyproject.toml` or `poetry` project for you with a sample PySpark application.

`init` is required to create those project types as it also initializes a `Dockerfile` used to package your dependencies. Single-file PySpark jobs and simple Python modules do not require the `init` command to be used.

### package

The `package` command bundles your PySpark code and dependencies in preparation for deployment. Often you'll either use `package` and `deploy` to deploy new artifacts to S3, or you'll use the `--build` flag in the `emr run` command to handle both of those tasks for you.

The EMR CLI automatically detects what type of project you have and builds the necessary dependency packages.

### deploy

The `deploy` command copies the project dependencies from the `dist/` folder to your specified S3 location.

### run

The `run` command is intended to help package, deploy, and run your PySpark code across EMR on EC2, EMR on EKS, or EMR Serverless.

You must provide one of `--cluster-id`, `--virtual-cluster-id`, or `--application-id` to specify which environment to run your code on.

`emr run --help` shows all the available options:

```
Usage: emr run [OPTIONS]

  Run a project on EMR, optionally build and deploy

Options:
  --application-id TEXT         EMR Serverless Application ID
  --cluster-id TEXT             EMR on EC2 Cluster ID
  --virtual-cluster-id TEXT     EMR on EKS Virtual Cluster ID
  --entry-point FILE            Python or Jar file for the main entrypoint
  --job-role TEXT               IAM Role ARN to use for the job execution
  --wait                        Wait for job to finish
  --s3-code-uri TEXT            Where to copy/run code artifacts to/from
  --s3-logs-uri TEXT            Where to send EMR Serverless logs to
  --job-name TEXT               The name of the job
  --job-args TEXT               Comma-delimited string of arguments to be
                                passed to Spark job

  --spark-submit-opts TEXT      String of spark-submit options
  --build                       Package and deploy job artifacts
  --show-stdout                 Show the stdout of the job after it's finished
  --save-config                 Update the config file with the provided
                                options

  --emr-eks-release-label TEXT  EMR on EKS release label (emr-6.15.0) -
                                defaults to latest release
```

## Support PySpark configurations

- Single-file project - Projects that have a single `.py` entrypoint file.
- Multi-file project - A more typical PySpark project, but without dependencies, that has multiple Python files or modules.
- Python module - A project with dependencies defined in a `pyproject.toml` file.
- Poetry project - A project using [Poetry](https://python-poetry.org/) for dependency management.

## Sample Commands

- Create a new PySpark project (other frameworks TBD)

```bash
emr init project-dir
```

- Package your project into a virtual environment archive

```bash
emr package --entry-point main.py
```

The EMR CLI auto-detects the project type and will change the packaging method appropriately.

If you have additional `.py` files, those will be included in the archive.

- Deploy an existing package artifact to S3.

```bash
emr deploy --entry-point main.py --s3-code-uri s3://<BUCKET>/code/
```

- Deploy a PySpark package to S3 and trigger an EMR Serverless job

```bash
emr run --entry-point main.py \
    --s3-code-uri s3://<BUCKET>/code/ \
    --application-id <EMR_SERVERLESS_APP> \
    --job-role <JOB_ROLE_ARN>
```

- Build, deploy, and run an EMR Serverless job and wait for it to finish.

```bash
emr run --entry-point main.py \
    --s3-code-uri s3://<BUCKET>/code/ \
    --application-id <EMR_SERVERLESS_APP> \
    --job-role <JOB_ROLE_ARN> \
    --build \
    --wait
```

- Re-run an already deployed job and show the `stdout` of the driver.

```bash
emr run --entry-point main.py \
    --s3-code-uri s3://<BUCKET>/code/ \
    --s3-logs-uri s3://<BUCKET>/logs/ \
    --application-id <EMR_SERVERLESS_APP> \
    --job-role <JOB_ROLE_ARN> \
    --show-stdout
```

> **Note**: If the job fails, the command will exit with an error code.

- Re-run your jobs with 7 characters.

If you provide the `--save-config` command to `emr run`, it will save a configuration file for you in `.emr/config.yaml` and next time you can use `emr run` with no parameters to re-run your job.

```bash
emr run --entry-point main.py \
    ... \
    --save-config

[emr-cli]: Config file saved to .emr/config.yaml. Use `emr run` to re-use your configuration.
```

```bash
❯ emr run
[emr-cli]: Using config file: .emr/config.yaml
```

🥳

- Run the same job against an EMR on EC2 cluster

```bash
emr run --entry-point main.py \
    --s3-code-uri s3://<BUCKET>/code/ \
    --s3-logs-uri s3://<BUCKET>/logs/ \
    --cluster-id <EMR_EC2_CLUSTER_ID>
    --show-stdout
```

- Or an EMR on EKS virtual cluster.

```bash
emr run --entry-point main.py \
    --s3-code-uri s3://<BUCKET>/code/ \
    --s3-logs-uri s3://<BUCKET>/logs/ \
    --virtual-cluster-id <EMR_EC2_CLUSTER_ID> \
    --job-role <EMR_EKS_JOB_ROLE_ARN> \
    --show-stdout
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
