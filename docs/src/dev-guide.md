# Development Guide

## Lance Java SDK Dependency

This package is dependent on the [Lance Java SDK](https://github.com/lancedb/lance/blob/main/java) and
[Lance Namespace Java Modules](https://github.com/lancedb/lance-namespace/tree/main/java).
You need to build those repositories locally first before building this repository.
If your have changes affecting those repositories,
the PR in `lancedb/lance-spark` will only pass CI after the PRs in `lancedb/lance` and `lance/lance-catalog` are merged.

## Build Commands

This connector is built using Maven. To build everything:

```shell
./mvnw clean install
```

To build everything without running tests:

```shell
./mvnw clean install -DskipTests
```

## Multi-Version Support

We offer the following build profiles for you to switch among different build versions:

- scala-2.12
- scala-2.13
- spark-3.4
- spark-3.5

For example, to use Scala 2.13:

```shell
./mvnw clean install -Pscala-2.13
```

To build a specific version like Spark 3.4:

```shell
./mvnw clean install -Pspark-3.4
```

To build only Spark 3.4:

```shell
./mvnw clean install -Pspark-3.4 -pl lance-spark-3.4 -am
```

Use the `shade-jar` profile to create the jar with all dependencies for Spark 3.4:

```shell
./mvnw clean install -Pspark-3.4 -Pshade-jar -pl lance-spark-3.4 -am
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

To apply spotless changes:

```shell
./mvnw spotless:apply
```

## Documentation Website

### Setup

The documentation website is built using [mkdocs-material](https://pypi.org/project/mkdocs-material).
The easiest way to setup is to create a Python virtual environment
and install the necessary dependencies:

```bash
python3 -m venv .env
source .env/bin/activate
pip install mkdocs-material
pip install mkdocs-awesome-pages-plugin
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