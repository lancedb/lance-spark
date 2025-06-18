# Config

## Spark Catalog Configuration

You should configure Spark with `com.lancedb.lance.spark.LanceCatalog` DSv2 catalog:

=== "PySpark"
```shell
pyspark \
  --packages com.lancedb:lance-spark-bundle-3.5_2.12:0.0.1 \
  --conf spark.sql.catalog.lance=com.lancedb.lance.spark.LanceCatalog
```

=== "Spark Shell (Scala)"
```shell
spark-shell \
  --packages com.lancedb:lance-spark-bundle-3.5_2.12:0.0.1 \
  --conf spark.sql.catalog.lance=com.lancedb.lance.spark.LanceCatalog
```

=== "Spark Submit"
```shell
spark-submit \
  --packages com.lancedb:lance-spark-bundle-3.5_2.12:0.0.1 \
  --conf spark.sql.catalog.lance=com.lancedb.lance.spark.LanceCatalog
```

=== "Spark SQL"
```shell
spark-sql \
  --packages com.lancedb:lance-spark-bundle-3.5_2.12:0.0.1 \
  --conf spark.sql.catalog.lance=com.lancedb.lance.spark.LanceCatalog
```

## Spark DataFrame Options

| Option              | Type    | Required? | Default | Description                          |
|---------------------|---------|-----------|---------|--------------------------------------|
| `db`                | String  | ✅         |         | Path to the Lance database directory |
| `dataset`           | String  | ✅         |         | Name of the Lance dataset            |

### Example

```java
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/lance/database")
    .option("dataset", "my_dataset")
    .load();
```