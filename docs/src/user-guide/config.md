# Configuration

Spark DSV2 catalog integrates with Lance through [Lance Namespace](https://github.com/lancedb/lance-namespace).

## Basic Setup

Configure Spark with the `LanceNamespaceSparkCatalog` by setting the appropriate Spark catalog implementation 
and namespace-specific options:

| Parameter                              | Type   | Required | Description                                                                                               |
|----------------------------------------|--------|----------|-----------------------------------------------------------------------------------------------------------|
| `spark.sql.catalog.{name}`             | String | ✓        | Set to `com.lancedb.lance.spark.LanceNamespaceSparkCatalog`                                               |
| `spark.sql.catalog.{name}.impl`        | String | ✓        | Namespace implementation, short name like `dir`, `rest`, `hive`, `glue` or full Java implementation class |

## Example Namespace Implementations

### Directory Namespace

=== "PySpark"
    ```python
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder \
        .appName("lance-dir-example") \
        .config("spark.sql.catalog.lance", "com.lancedb.lance.spark.LanceNamespaceSparkCatalog") \
        .config("spark.sql.catalog.lance.impl", "dir") \
        .config("spark.sql.catalog.lance.root", "/path/to/lance/database") \
        .getOrCreate()
    ```

=== "Scala"
    ```scala
    import org.apache.spark.sql.SparkSession
    
    val spark = SparkSession.builder()
        .appName("lance-dir-example")
        .config("spark.sql.catalog.lance", "com.lancedb.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "dir")
        .config("spark.sql.catalog.lance.root", "/path/to/lance/database")
        .getOrCreate()
    ```

=== "Java"
    ```java
    import org.apache.spark.sql.SparkSession;
    
    SparkSession spark = SparkSession.builder()
        .appName("lance-dir-example")
        .config("spark.sql.catalog.lance", "com.lancedb.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "dir")
        .config("spark.sql.catalog.lance.root", "/path/to/lance/database")
        .getOrCreate();
    ```

=== "Spark Shell"
    ```shell
    spark-shell \
      --packages com.lancedb:lance-spark-bundle-3.5_2.12:0.0.1 \
      --conf spark.sql.catalog.lance=com.lancedb.lance.spark.LanceNamespaceSparkCatalog \
      --conf spark.sql.catalog.lance.impl=dir \
      --conf spark.sql.catalog.lance.root=/path/to/lance/database
    ```

=== "Spark Submit"
    ```shell
    spark-submit \
      --packages com.lancedb:lance-spark-bundle-3.5_2.12:0.0.1 \
      --conf spark.sql.catalog.lance=com.lancedb.lance.spark.LanceNamespaceSparkCatalog \
      --conf spark.sql.catalog.lance.impl=dir \
      --conf spark.sql.catalog.lance.root=/path/to/lance/database \
      your-application.jar
    ```

### REST Namespace

Here we use LanceDB Cloud as an example of the REST namespace:

=== "PySpark"
    ```python
    spark = SparkSession.builder \
        .appName("lance-rest-example") \
        .config("spark.sql.catalog.lance", "com.lancedb.lance.spark.LanceNamespaceSparkCatalog") \
        .config("spark.sql.catalog.lance.impl", "rest") \
        .config("spark.sql.catalog.lance.headers.x-api-key", "your-api-key") \
        .config("spark.sql.catalog.lance.headers.x-lancedb-database", "your-database") \
        .config("spark.sql.catalog.lance.uri", "https://your-database.us-east-1.api.lancedb.com") \
        .getOrCreate()
    ```

=== "Scala"
    ```scala
    val spark = SparkSession.builder()
        .appName("lance-rest-example")
        .config("spark.sql.catalog.lance", "com.lancedb.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "rest")
        .config("spark.sql.catalog.lance.headers.x-api-key", "your-api-key")
        .config("spark.sql.catalog.lance.headers.x-lancedb-database", "your-database")
        .config("spark.sql.catalog.lance.uri", "https://your-database.us-east-1.api.lancedb.com")
        .getOrCreate()
    ```

=== "Java"
    ```java
    SparkSession spark = SparkSession.builder()
        .appName("lance-rest-example")
        .config("spark.sql.catalog.lance", "com.lancedb.lance.spark.LanceNamespaceSparkCatalog")
        .config("spark.sql.catalog.lance.impl", "rest")
        .config("spark.sql.catalog.lance.headers.x-api-key", "your-api-key")
        .config("spark.sql.catalog.lance.headers.x-lancedb-database", "your-database")
        .config("spark.sql.catalog.lance.uri", "https://your-database.us-east-1.api.lancedb.com")
        .getOrCreate();
    ```

=== "Spark Shell"
    ```shell
    spark-shell \
      --packages com.lancedb:lance-spark-bundle-3.5_2.12:0.0.1 \
      --conf spark.sql.catalog.lance=com.lancedb.lance.spark.LanceNamespaceSparkCatalog \
      --conf spark.sql.catalog.lance.impl=rest \
      --conf spark.sql.catalog.lance.headers.x-api-key=your-api-key \
      --conf spark.sql.catalog.lance.headers.x-lancedb-database=your-database \
      --conf spark.sql.catalog.lance.uri=https://your-database.us-east-1.api.lancedb.com
    ```

=== "Spark Submit"
    ```shell
    spark-submit \
      --packages com.lancedb:lance-spark-bundle-3.5_2.12:0.0.1 \
      --conf spark.sql.catalog.lance=com.lancedb.lance.spark.LanceNamespaceSparkCatalog \
      --conf spark.sql.catalog.lance.impl=rest \
      --conf spark.sql.catalog.lance.headers.x-api-key=your-api-key \
      --conf spark.sql.catalog.lance.headers.x-lancedb-database=your-database \
      --conf spark.sql.catalog.lance.uri=https://your-database.us-east-1.api.lancedb.com \
      your-application.jar
    ```

### Note on Namespace Levels

Spark provides at least a 3 level hierarchy of **catalog → multi-level namespace → table**
(most users treat as a 3 level hierarchy with 1 level namespace).
Since Lance allows a 2 level hierarchy of **root namespace → table** for namespaces like `DirectoryNamespace`,
the `LanceNamespaceSparkCatalog` provides a configuration `extra_level` which puts an additional dummy level
to match the Spark hierarchy and make it **root namespace → dummy extra level → table**.

Currently, this is automatically set with `extra_level=default` for `DirectoryNamespace`
and when `RestNamespace` if it cannot respond to `ListNamespaces` operation.
If you have a custom namespace implementation of the same behavior, you can also set the config to add the extra level.


