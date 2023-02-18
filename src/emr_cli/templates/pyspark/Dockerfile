# Base stage for python development
FROM --platform=linux/amd64 amazonlinux:2 AS base

RUN yum install -y python3 tar gzip

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

RUN python3 -m pip install --upgrade pip
RUN curl -sSL https://install.python-poetry.org | python3 -

ENV PATH="$PATH:/root/.local/bin"

WORKDIR /app

COPY . .

# Test stage - installs test dependencies defined in pyproject.toml
FROM base as test
RUN python3 -m pip install .[test]

# Build stage - installs required dependencies and creates a venv package
FROM base as build
RUN python3 -m pip install venv-pack==0.2.0 && \
    python3 -m pip install .
RUN mkdir /output && venv-pack -o /output/pyspark_deps.tar.gz

# Build stage for poetry
FROM base as poetry-build
RUN poetry self add poetry-plugin-bundle && \
    poetry bundle venv dist/bundle --without dev && \
    tar -czvf dist/pyspark_deps.tar.gz -C dist/bundle . && \
    rm -rf dist/bundle

FROM scratch as export-poetry
COPY --from=poetry-build /app/dist/pyspark_deps.tar.gz /

# Export stage - used to copy packaged venv to local filesystem
FROM scratch AS export
COPY --from=build /output/pyspark_deps.tar.gz /