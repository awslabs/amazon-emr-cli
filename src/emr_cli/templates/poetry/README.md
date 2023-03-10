# EMR Serverless Poetry Template

Welcome to your new EMR Serverless Poetry PySpark project!

To get started, change into the project you just created and run the `install` command.

```bash
poetry install
```

Your dependencies should now all be resolved and yo ushould have a new `poetry.lock` file in your project.

## Deploy!

Now we can go ahead and build our project and deploy it on EMR Serverless.

> **Note** This tutorial assumes you have already [setup EMR Serverless](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/setting-up.html) and have an EMR Serverless application, job role, and S3 bucket you can use. You can also use the `emr bootstrap` command.

1. Set your relevant variables

```bash
APPLICATION_ID=<emr-serverless-app-id>
JOB_ROLE_ARN=<emr-serverless-job-role>
S3_BUCKET=<s3-bucket-name>
```

2. Package, deploy, and run your job all in one command.

```
emr run \
    --entry-point entrypoint.py \
    --application-id ${APPLICATION_ID} \
    --job-role ${JOB_ROLE_ARN} \
    --s3-code-uri  s3://${S3_BUCKET}/tmp/emr-cli-demo-poetry/ \
    --build --wait
```