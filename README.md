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

* **Read & Write Lance Datasets**: Seamlessly read and write datasets stored in the Lance format using Spark.
* **Distributed, Parallel Scans**: Leverage Spark's distributed computing capabilities to perform parallel scans on Lance datasets.
* **Column and Filter Pushdown**: Optimize query performance by pushing down column selections and filters to the data source.

## Contributing Guide

See [contributing](docs/src/contributing.md) for the detailed contribution guidelines and local development setup.

