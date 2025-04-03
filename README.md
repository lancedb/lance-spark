# Apache Spark Connector for Lance

The Apache Spark Connector for Lance allows Apache Spark to efficiently read datasets stored in Lance format.

Lance is a modern columnar data format optimized for machine learning workflows and datasets,
supporting distributed, parallel scans, and optimizations such as column and filter pushdown to improve performance.
Additionally, Lance provides high-performance random access that is 100 times faster than Parquet 
without sacrificing scan performance.

By using the Apache Spark Connector for Lance, you can leverage Spark's powerful data processing, SQL querying, 
and machine learning training capabilities on the AI data lake powered by Lance.

## Features

The connector is built using the Spark DatasourceV2 API. 
Please check the [this presentation](https://www.slideshare.net/databricks/apache-spark-data-source-v2-with-wenchen-fan-and-gengliang-wang)
to learn more about its features.
Specifically, you can use the Apache Spark Connector for Lance to:

* **Query Lance Datasets**: Seamlessly query datasets stored in the Lance format using Spark.
* **Distributed, Parallel Scans**: Leverage Spark's distributed computing capabilities to perform parallel scans on Lance datasets.
* **Column and Filter Pushdown**: Optimize query performance by pushing down column selections and filters to the data source.

## Installation

### Requirements

Java: 8, 11, 17
Scala: 2.12
Spark: 3.4, 3.5
Operating System: Linux x86, macOS

### Download jar

Jars with full dependency are uploaded in public S3 bucket `spark-lance-artifacts`,
with name pattern `lance-spark-{spark-version}-{scala-version}-{connector-version}-jar-with-dependencies.jar`.
FOr example, to get Spark 3.5 Scala 2.12 jar for connector version 0.1.0, do:

```shell
wget https://spark-lance-artifacts.s3.amazonaws.com/lance-spark-3.5-2.12-0.1.0-jar-with-dependencies.jar
```

## Quick Start

Launch `spark-shell` with your selected JAR according to your Spark and Scala version:

```shell
spark-shell --jars lance-spark-3.5-2.12-0.1.0-jar-with-dependencies.jar
```

Example Usage:

```java
import org.apache.spark.sql.SparkSession;

SparkSession spark = SparkSession.builder()
    .appName("spark-lance-connector-test")
    .master("local")
    .getOrCreate();

Dataset<Row> data = spark.read()
    .format("lance")
    .option("db", "/path/to/example_db")
    .option("dataset", "lance_example_dataset")
    .load();

data.show(100);
```

More examples can be found in [SparkLanceConnectorReadTest](/lance-spark-base/src/test/java/com/lancedb/lance/spark/read/SparkConnectorReadTestBase.java).

## Development Guide

### Lance Java SDK Dependency

This package is dependent on the Lance Java SDK.
You need to follow [its guide](https://github.com/lancedb/lance/blob/main/java/README.md) to first build it locally.

### Build Commands

This connector is built using Maven. To build everything:

```shell
./mvnw clean install
```

To build everything without running tests:

```shell
./mvnw clean install -DskipTests
```

### Multi-Version Support

We offer the following build profiles for you to switch among different build versions:

- scala-2.12
- scala-2.13
- spark-3.4
- spark-3.5

For example, to use Scala 2.13:

```shell
./mvnw clean install -Pscala-2.13
```

To use Spark 3.4:

```shell
./mvnw clean install -Pspark-3.4
```

To build only Spark 3.4:

```shell
./mvnw clean install -Pspark-3.4 -pl lance-spark-3.4 -am
```

To create the jar with all dependencies for Spark 3.4:

```shell
./mvnw clean install -Pspark-3.4 -Pshade-jar -pl lance-spark-3.4 -am
```