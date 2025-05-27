# Apache Spark Connector for Lance

The Apache Spark Connector for Lance allows Apache Spark to efficiently read datasets stored in Lance format.

Lance is a modern columnar data format optimized for machine learning workflows and datasets, supporting distributed, parallel scans, and optimizations such as column and filter pushdown to improve performance. Additionally, Lance provides high-performance random access that is 100 times faster than Parquet without sacrificing scan performance.

By using the Apache Spark Connector for Lance, you can leverage Apache Spark's powerful data processing, SQL querying, and machine learning training capabilities on the AI data lake powered by Lance.

## Key Features

The connector is built using the Spark DatasourceV2 (DSv2) API. Please check [this presentation](https://www.slideshare.net/databricks/apache-spark-data-source-v2-with-wenchen-fan-and-gengliang-wang) to learn more about DSv2 features.

Specifically, you can use the Apache Spark Connector for Lance to:

- **Query Lance Datasets**: Seamlessly query datasets stored in the Lance format using Spark
- **Distributed, Parallel Scans**: Leverage Spark's distributed computing capabilities to perform parallel scans on Lance datasets
- **Column and Filter Pushdown**: Optimize query performance by pushing down column selections and filters to the data source

## Quick Start

Launch `spark-shell` with your selected JAR according to your Spark and Scala version:

```shell
spark-shell --packages com.lancedb.lance:lance-spark-bundle-3.5_2.12:0.0.1
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

## What's Next?

- [Installation Guide](getting-started/installation.md) - Learn how to install and set up the connector
- [Quick Start Guide](getting-started/quick-start.md) - Get up and running quickly
- [User Guide](user-guide/reading-datasets.md) - Detailed usage instructions
- [API Reference](reference/api.md) - Complete API documentation

## About Lance

Lance is a modern columnar data format that is optimized for ML workflows and datasets. Lance is perfect for:

1. Building search engines and feature stores
2. Large-scale ML training requiring high performance IO and shuffles
3. Storing, querying, and inspecting deeply nested data for robotics or large blobs like images, point clouds, and more

The key features of Lance include:

- **High-performance random access**: 100x faster than Parquet without sacrificing scan performance
- **Vector search**: Find nearest neighbors in milliseconds and combine OLAP-queries with vector search
- **Zero-copy, automatic versioning**: Manage versions of your data without needing extra infrastructure
- **Ecosystem integrations**: Apache Arrow, Pandas, Polars, DuckDB, Ray, Spark and more on the way

## Community

- **GitHub**: [lancedb/lance-spark](https://github.com/lancedb/lance-spark)
- **Discord**: [Join our community](https://discord.gg/zMM32dvNtd)
- **Twitter**: [@lancedb](https://twitter.com/lancedb) 