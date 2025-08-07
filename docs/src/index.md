# Welcome

![logo](./logo/wide.png)

## Introduction

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

## Quick Start

### Start Session

=== "PySpark"
    ```shell
    pyspark --packages com.lancedb:lance-spark-bundle-3.5_2.12:0.0.5
    ```

=== "Spark Shell (Scala)"
    ```shell
    spark-shell --packages com.lancedb:lance-spark-bundle-3.5_2.12:0.0.5
    ```

### Prepare Data

=== "Python"
    ```python
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("score", FloatType(), False),
    ])
    
    data = [
        (1, "Alice", 85.5),
        (2, "Bob", 92.0),
        (3, "Carol", 78.0),
    ]
    
    df = spark.createDataFrame(data, schema=schema)
    ```

=== "Scala"
    ```scala
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types._

    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("score", FloatType, nullable = false)
    ))
    
    val data = Seq(
      Row(1, "Alice", 85.5f),
      Row(2, "Bob", 92.0f),
      Row(3, "Carol", 78.0f)
    )
    
    val rdd = spark.sparkContext.parallelize(data)
    val df = spark.createDataFrame(rdd, schema)
    ```


### Simple Write

=== "Python"
    ```python
    (df.write
        .format("lance")
        .mode("overwrite")
        .save("/tmp/manual_users.lance"))
    ```

=== "Scala"
    ```scala
    df.write.
        format("lance").
        mode("overwrite").
        save("/tmp/manual_users.lance")
    ```

### Simple Read

=== "Python"
    ```python
    (spark.read
        .format("lance")
        .load("/tmp/manual_users.lance")
        .show())
    ```

=== "Scala"
    ```scala
    spark.read.
        format("lance").
        load("/tmp/manual_users.lance").
        show()
    ```

### Docker

The project contains a docker image in the `docker` folder you can build and run a simple example notebook.
To do so, clone the repo and run:

```shell
make docker-build
make docker-up
```

And then open the notebook at `http://localhost:8888`.