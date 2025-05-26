# Apache Spark Connector for Lance

The Apache Spark Connector for Lance allows Apache Spark to efficiently read datasets stored in Lance format.

Lance is a modern columnar data format optimized for machine learning workflows and datasets,
supporting distributed, parallel scans, and optimizations such as column and filter pushdown to improve performance.
Additionally, Lance provides high-performance random access that is 100 times faster than Parquet 
without sacrificing scan performance.

By using the Apache Spark Connector for Lance, you can leverage Apache Spark's powerful data processing, SQL querying, 
and machine learning training capabilities on the AI data lake powered by Lance.

## Features

The connector is built using the Spark DatasourceV2 (DSv2) API. 
Please check [this presentation](https://www.slideshare.net/databricks/apache-spark-data-source-v2-with-wenchen-fan-and-gengliang-wang)
to learn more about DSv2 features.
Specifically, you can use the Apache Spark Connector for Lance to:

* **Query Lance Datasets**: Seamlessly query datasets stored in the Lance format using Spark.
* **Distributed, Parallel Scans**: Leverage Spark's distributed computing capabilities to perform parallel scans on Lance datasets.
* **Column and Filter Pushdown**: Optimize query performance by pushing down column selections and filters to the data source.

## Installation

### Requirements

| Requirement | Supported Versions                         |
|-------------|--------------------------------------------|
| Java        | 8, 11, 17                                  |
| Scala       | 2.12                                       |
| Spark       | 3.4, 3.5                                   |
| OS          | Any OS that is supported by Lance Java SDK |

### Maven Central

The connector packages are published to Maven Central under `com.lancedb` namespace:

| Artifact Type | Name Pattern                                         | Description                                                                                                                                     | Example                     |
|---------------|------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------|
| Base Jar      | `lance-spark-base_<scala_version>`                   | Jar with logic shared by different versions of Spark Lance connectors, only intended for internal use.                                          | lance-spark-base_2.12       |
| Lean Jar      | `lance-spark-<spark-version>_<scala_version>`        | Jar with only the Spark Lance connector logic, suitable for building a Spark application which you will re-bundle later with other dependencies | lance-spark-3.5_2.12        |
| Bundled Jar   | `lance-spark-bundle-<spark-version>_<scala_version>` | Jar with all necessary non-Spark dependencies, suitable for use directly in a Spark session                                                     | lance-spark-bundle-3.5_2.12 |

## Quick Start

Launch `spark-shell` with your selected JAR according to your Spark and Scala version:

```shell
spark-shell --packages com.lancedb.lance:lance-spark-bundle-3.5_2.12:0.1.0
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

This package is dependent on the [Lance Java SDK](https://github.com/lancedb/lance/blob/main/java) and 
[Lance Catalog Java Modules](https://github.com/lancedb/lance-catalog/tree/main/java).
You need to build those repositories locally first before building this repository.
If your have changes affecting those repositories,
the PR in `lancedb/lance-spark` will only pass CI after the PRs in `lancedb/lance` and `lance/lance-catalog` are merged.

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

### Styling Guide

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
