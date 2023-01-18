# Base stage for python development
FROM --platform=linux/amd64 amazonlinux:2 AS base

RUN yum install -y python3

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

WORKDIR /app

COPY . .
RUN python3 -m pip install --upgrade pip

# Test stage - installs test dependencies defined in pyproject.toml
FROM base as test
RUN python3 -m pip install .[test]

# Build stage - installs required dependencies and creates a venv package
FROM base as build
RUN python3 -m pip install venv-pack==0.2.0 && \
    python3 -m pip install .
RUN mkdir /output && venv-pack -o /output/pyspark_deps.tar.gz

# Export stage - used to copy packaged venv to local filesystem
FROM scratch AS export
COPY --from=build /output/pyspark_deps.tar.gz /