# Quick Start

This guide will get you up and running with the Apache Spark Connector for Lance in just a few minutes.

## Prerequisites

Before you begin, make sure you have:

- Java 8, 11, or 17 installed
- Apache Spark 3.5 installed
- Scala 2.12

If you haven't installed the connector yet, see the [Installation Guide](installation.md).

## Starting Spark Shell

Launch `spark-shell` with the Lance connector:

```shell
spark-shell --packages com.lancedb.lance:lance-spark-bundle-3.5_2.12:0.0.1
```

This will download the connector and all its dependencies automatically.

## Basic Usage

### Reading a Lance Dataset

Once Spark shell is running, you can read Lance datasets using the following pattern:

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

### Configuration Options

The connector supports several configuration options:

| Option | Description | Required |
|--------|-------------|----------|
| `db` | Path to the Lance database directory | Yes |
| `dataset` | Name of the Lance dataset | Yes |

### Example with Real Data

Here's a complete example that demonstrates reading a Lance dataset:

```java
// Create Spark session
SparkSession spark = SparkSession.builder()
    .appName("lance-example")
    .master("local[*]")
    .getOrCreate();

// Read Lance dataset
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/your/lance/db")
    .option("dataset", "your_dataset_name")
    .load();

// Show schema
df.printSchema();

// Show first 20 rows
df.show();

// Count total rows
System.out.println("Total rows: " + df.count());

// Apply some transformations
Dataset<Row> filtered = df.filter(df.col("some_column").gt(100));
filtered.show();

// Write results (to other formats)
filtered.write()
    .mode("overwrite")
    .parquet("/path/to/output");
```

## Working with DataFrames

Once you've loaded a Lance dataset, you can use all standard Spark DataFrame operations:

### Filtering Data

```java
// Filter rows based on conditions
Dataset<Row> filtered = df.filter(df.col("age").gt(25));

// Multiple conditions
Dataset<Row> complex = df.filter(
    df.col("age").gt(25).and(df.col("city").equalTo("New York"))
);
```

### Selecting Columns

```java
// Select specific columns
Dataset<Row> selected = df.select("name", "age", "city");

// Select with expressions
Dataset<Row> computed = df.select(
    df.col("name"),
    df.col("age").plus(1).alias("next_age")
);
```

### Aggregations

```java
// Group by and aggregate
Dataset<Row> grouped = df.groupBy("city")
    .agg(
        functions.count("*").alias("count"),
        functions.avg("age").alias("avg_age")
    );
```

## SQL Queries

You can also use SQL to query Lance datasets:

```java
// Register as temporary view
df.createOrReplaceTempView("people");

// Run SQL queries
Dataset<Row> result = spark.sql(
    "SELECT city, COUNT(*) as count, AVG(age) as avg_age " +
    "FROM people " +
    "WHERE age > 25 " +
    "GROUP BY city " +
    "ORDER BY count DESC"
);

result.show();
```

## Performance Tips

### Column Pushdown

The connector automatically pushes down column selections to improve performance:

```java
// Only reads the specified columns from Lance
Dataset<Row> efficient = df.select("name", "age");
```

### Filter Pushdown

Filters are also pushed down to the Lance layer:

```java
// Filter is applied at the Lance level, not in Spark
Dataset<Row> filtered = df.filter(df.col("age").gt(25));
```

## Common Patterns

### Reading Multiple Datasets

```java
// Read multiple datasets and union them
Dataset<Row> dataset1 = spark.read()
    .format("lance")
    .option("db", "/path/to/db")
    .option("dataset", "dataset1")
    .load();

Dataset<Row> dataset2 = spark.read()
    .format("lance")
    .option("db", "/path/to/db")
    .option("dataset", "dataset2")
    .load();

Dataset<Row> combined = dataset1.union(dataset2);
```

### Joining with Other Data Sources

```java
// Join Lance data with Parquet
Dataset<Row> lanceData = spark.read()
    .format("lance")
    .option("db", "/path/to/lance/db")
    .option("dataset", "users")
    .load();

Dataset<Row> parquetData = spark.read()
    .parquet("/path/to/parquet/file");

Dataset<Row> joined = lanceData.join(parquetData, "user_id");
```

## Next Steps

Now that you've got the basics down, explore these topics:

- [Reading Datasets](../user-guide/reading-datasets.md) - Advanced reading techniques
- [Configuration](../user-guide/configuration.md) - Detailed configuration options
- [Performance Tuning](../user-guide/performance.md) - Optimize your queries
- [Examples](../user-guide/examples.md) - More comprehensive examples

## Troubleshooting

If you encounter issues:

1. **ClassNotFoundException**: Make sure the connector JAR is in your classpath
2. **Dataset not found**: Verify the `db` and `dataset` paths are correct
3. **Performance issues**: Check the [Performance Guide](../user-guide/performance.md)

For more help, see the [Troubleshooting Guide](../reference/troubleshooting.md). 