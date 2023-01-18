# EMR CLI

So we're all working on data pipelines every day, but wouldn't be nice to just hit a button and have our code automatically deployed to production or test accounts I thought so, too, thats why I created the EMR CLI (`emr`) that can help you package and deploy your EMR jobs so you don't have to.

The EMR CLI supports a wide variety of configuration options to adapt to _your_ data pipeline, not the other way around.

1. Packaging - Ensure a consistent approach to packaging your production Spark jobs.
2. Deployment - Easily deploy your Spark jobs across multiple EMR environments or deployment frameworks like EC2, EKS, and Serverless.
3. CI/CD - Easily test each iteration of your code without resorting to messy shell scripts. :)

## pyspark code

In many organizations, PySpark is the primary language for writing Spark jobs. But Python projects can be structured in a variety of ways – a single `.py` file, `requirements.txt`, `setup.py` files, or even `poetry` configurations. EMR CLI aims to bundle your PySpark code the same way regardless of which system you use.

## Spark scala code

While Spark Scala or Java code will be more standard from a packaging perspective, it's still useful to able to easily deploy and run your jobs across multiple EMR environments.

## Spark SQL

Want to just write some `.sql` files and have those deployed? No problem.

## Sample Commands

- Prepare a PySpark package with a single PySpark file.

```shell
emr package --target pyspark --version v0.0.1 --source ./jobs/main.py`
```

- Provide multiple files (with the entrypoint first) to include local dependencies

```shell
emr package --target pyspark --version v0.0.1 --source ./jobs/main.py ./jobs/lib/*
```

- Prepare a PySpark package with a setup.py file (`--version` is inferred from `setup.py`).

```shell
emr package --target pyspark --source setup.py
```

- Package and Deploy a PySpark package to S3

```shell
emr deploy --target pyspark
```

- Package, deploy, and run a PySpark package on EMR Serverless

```shell
emr run --target pyspark --source setup.py --s3-code-uri s3://your-bucket/code/pyspark/ --application-id 1234567890
```

Note that if you've previously run package or deploy commands, other commands can be abbreviated.

```shell
emr run --application-id 1234567890
```

You can also utilize the same code against an EMR on EC2 cluster

```shell
emr run --cluster-id j-8675309
```

Or an EMR on EKS virtual cluster.

```shell
emr run --virtual-cluster-id 654abacdefgh1uziuyackhrs1
```

## Actual ToDo

The first thing I want to do is make my `integration-test.sh` script unnecessary. 

1. emr deploy s3://bucket/code/
    - single `.py` file in your dirtree?
    - copy
2. emr run --wait --application-id 123456
    - ask for an emr iam role
3. Figure out versioning


`deploy` command detects single/multiple pyspark deployments
`run` command needs IAM role as well