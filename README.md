# EMR 1-Up

So we're all working on data pipelines every day, but wouldn't be nice to just hit a button and have our code automatically deployed to production or test accounts I thought so, too, thats why I created the EMR 1-up tool (emr1up) that can help you package and deploy your EMR jobs so you don't have to.

emr1up supports a wide variety of configuration options to adapt to _your_ data pipeline, not the other way around.

1. Packaging - Ensure a consistent approach to packaging your production Spark jobs.
2. Deployment - Easily deploy your Spark jobs across multiple EMR environments or deployment frameworks like EC2, EKS, and Serverless.
3. CI/CD - Easily test each iteration of your code without resorting to messy shell scripts. :)

## pyspark code

In many organizations, PySpark is the primary language for writing Spark jobs. But Python projects can be structured in a variety of ways – a single `.py` file, `requirements.txt`, `setup.py` files, or even `poetry` configurations. emr1up aims to bundle your PySpark code the same way regardless of which system you use.

## Spark scala code

While Spark Scala or Java code will be more standard from a packaging perspective, it's still useful to able to easily deploy and run your jobs across multiple EMR environments.

## Spark SQL

Want to just write some `.sql` files and have those deployed? No problem.

## Sample Commands

- Prepare a PySpark package with a single PySpark file.

```shell
emr1up package --target pyspark --version v0.0.1 --source ./jobs/main.py`
```

- Provide multiple files (with the entrypoint first) to include local dependencies

```shell
emr1up package --target pyspark --version v0.0.1 --source ./jobs/main.py ./jobs/lib/*
```

- Prepare a PySpark package with a setup.py file (`--version` is inferred from `setup.py`).

```shell
emr1up package --target pyspark --source setup.py
```

- Package and Deploy a PySpark package to S3

```shell
emr1up deploy --target pyspark --source setup.py --s3-code-uri s3://your-bucket/code/pyspark/
```

- Package, deploy, and run a PySpark package on EMR Serverless

```shell
emr1up run --target pyspark --source setup.py --s3-code-uri s3://your-bucket/code/pyspark/ --application-id 1234567890
```

Note that if you've previously run package or deploy commands, other commands can be abbreviated.

```shell
emr1up run --application-id 1234567890
```

You can also utilize the same code against an EMR on EC2 cluster

```shell
emr1up run --cluster-id j-8675309
```

Or an EMR on EKS virtual cluster.

```shell
emr1up run --virtual-cluster-id 654abacdefgh1uziuyackhrs1
```

## Actual ToDo

The first thing I want to do is make my `integration-test.sh` script unnecessary. 

1. emr1up deploy s3://bucket/code/
2. emr1up run --wait --application-id 123456


`deploy` command detects single/multiple pyspark deployments
`run` command needs IAM role as well