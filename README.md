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

> **Note** This tutorial assumes you have already [setup EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/setting-up.html) and have an EMR Serverless application, job role, and S3 bucket you can use. You can also use the `emr bootstrap` command.

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
    --build \
    --wait
```

This command performs the following actions:

- Packages your project dependencies into a python virtual environment
- Uploads the Spark entrypoint and packaged dependencies to S3
- Starts an EMR Serverless job
- Waits for the job to run to a successful completion!

And you're done. Feel free to modify the project to experiment with different things. You can simply re-run the command above to re-package and re-deploy your job.

## pyspark code

In many organizations, PySpark is the primary language for writing Spark jobs. But Python projects can be structured in a variety of ways – a single `.py` file, `requirements.txt`, `setup.py` files, or even `poetry` configurations. EMR CLI aims to bundle your PySpark code the same way regardless of which system you use.

## Spark scala code (coming)

While Spark Scala or Java code will be more standard from a packaging perspective, it's still useful to able to easily deploy and run your jobs across multiple EMR environments.

## Spark SQL (coming)

Want to just write some `.sql` files and have those deployed? No problem.

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

> **Note**: If the job fails, the command will exit with an error code.

In the future, you'll also be able to do the following:

- Utilize the same code against an EMR on EC2 cluster

```bash
emr run --cluster-id j-8675309
```

- Or an EMR on EKS virtual cluster.

```bash
emr run --virtual-cluster-id 654abacdefgh1uziuyackhrs1
```

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.
