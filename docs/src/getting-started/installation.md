# Installation

This guide will help you install and set up the Apache Spark Connector for Lance.

## Requirements

Before installing the connector, ensure your environment meets the following requirements:

| Requirement | Supported Versions                         |
|-------------|--------------------------------------------|
| Java        | 8, 11, 17                                  |
| Scala       | 2.12                                       |
| Spark       | 3.5                                        |
| OS          | Any OS that is supported by Lance Java SDK |

!!! note "Operating System Support"
    The connector supports any operating system that is compatible with the Lance Java SDK. This includes Linux, macOS, and Windows.

## Maven Central Packages

The connector packages are published to Maven Central under the `com.lancedb` namespace. Choose the appropriate artifact based on your use case:

### Available Artifacts

| Artifact Type | Name Pattern                                         | Description                                                                                                                                     | Example                     |
|---------------|------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------|
| Base Jar      | `lance-spark-base_<scala_version>`                   | Jar with logic shared by different versions of Spark Lance connectors, only intended for internal use.                                          | lance-spark-base_2.12       |
| Lean Jar      | `lance-spark-<spark-version>_<scala_version>`        | Jar with only the Spark Lance connector logic, suitable for building a Spark application which you will re-bundle later with other dependencies | lance-spark-3.5_2.12        |
| Bundled Jar   | `lance-spark-bundle-<spark-version>_<scala_version>` | Jar with all necessary non-Spark dependencies, suitable for use directly in a Spark session                                                     | lance-spark-bundle-3.5_2.12 |

### Choosing the Right Artifact

- **Bundled Jar**: Recommended for most users. Use this if you want to quickly get started or use the connector in a Spark shell/notebook environment.
- **Lean Jar**: Use this if you're building a custom Spark application and want to manage dependencies yourself.
- **Base Jar**: Internal use only. Don't use this directly.

## Installation Methods

### Using Spark Shell

The easiest way to get started is using the bundled JAR with `spark-shell`:

```shell
spark-shell --packages com.lancedb.lance:lance-spark-bundle-3.5_2.12:0.0.1
```

### Using Spark Submit

For Spark applications, you can include the connector using `spark-submit`:

```shell
spark-submit --packages com.lancedb.lance:lance-spark-bundle-3.5_2.12:0.0.1 your-app.jar
```

### Maven Configuration

If you're building a Maven project, add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.lancedb.lance</groupId>
    <artifactId>lance-spark-bundle-3.5_2.12</artifactId>
    <version>0.0.1</version>
</dependency>
```

### SBT Configuration

For SBT projects, add this to your `build.sbt`:

```scala
libraryDependencies += "com.lancedb.lance" %% "lance-spark-bundle-3.5" % "0.0.1"
```

### Gradle Configuration

For Gradle projects, add this to your `build.gradle`:

```gradle
dependencies {
    implementation 'com.lancedb.lance:lance-spark-bundle-3.5_2.12:0.0.1'
}
```

## Verification

To verify that the installation was successful, you can run a simple test:

```java
import org.apache.spark.sql.SparkSession;

SparkSession spark = SparkSession.builder()
    .appName("lance-connector-test")
    .master("local")
    .getOrCreate();

// Try to create a DataFrameReader with lance format
spark.read().format("lance");
```

If no exceptions are thrown, the connector is properly installed and ready to use.

## Next Steps

- [Quick Start Guide](quick-start.md) - Get up and running with your first Lance dataset
- [Requirements](requirements.md) - Detailed system requirements
- [Reading Datasets](../user-guide/reading-datasets.md) - Learn how to read Lance datasets 