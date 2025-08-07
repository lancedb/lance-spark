# Contributing to Spark Lance Connector

This is the contribution guide for [Spark Lance Connector](https://github.com/lancedb/lance-spark)

## Build Commands

This connector is built using Maven. You can run the following make commands:

```shell
# Build all
make build

# Clean all
make clean

# Build Spark 3.5 Scala 2.12
make build-35-212

# Clean build of Spark 3.5 Scala 2.12
make clean-35-212

# Build the runtime bundle of Spark 3.5 Scala 2.12
make bundle-35-212
```

To build everything without running tests:

```shell
./mvnw clean install -DskipTests
```

## Styling Guide

We use checkstyle and spotless to lint the code.

To verify checkstyle:

```shell
./mvnw checkstyle:check
```

To verify spotless:

```shell
./mvnw spotless:check
```

All the `make build` commands automatically perform `spotless:apply` to the code.

## Documentation Website

### Setup

The documentation website is built using [mkdocs-material](https://pypi.org/project/mkdocs-material).

Install dependencies with:

```shell
uv pip install -r requirements.txt
```

Run the website with


The easiest way to setup is to create a Python virtual environment
and install the necessary dependencies:

```bash
python3 -m venv .env
source .env/bin/activate
pip install 
pip install 
```

Then you can start the server at `http://localhost:8000/lance-spark` by:

```bash
source .env/bin/activate
mkdocs serve
```

### Contents

In general, we push most of the contents in the website as the single source of truth.
The welcome page is the same as the README of the GitHub repository.
If you edit one of them, please make sure to update the other document.