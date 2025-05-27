# Reading Lance Datasets

This guide covers everything you need to know about reading Lance datasets using the Apache Spark Connector for Lance.

## Basic Reading

### Simple Dataset Reading

The most basic way to read a Lance dataset:

```java
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/lance/database")
    .option("dataset", "my_dataset")
    .load();
```

### Required Options

| Option | Description | Example |
|--------|-------------|---------|
| `db` | Path to the Lance database directory | `/data/lance_db` |
| `dataset` | Name of the Lance dataset within the database | `users` |

## Advanced Reading Options

### Schema Inference

By default, the connector automatically infers the schema from the Lance dataset:

```java
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/lance/database")
    .option("dataset", "my_dataset")
    .load();

// Print the inferred schema
df.printSchema();
```

### Column Selection

You can specify which columns to read to improve performance:

```java
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/lance/database")
    .option("dataset", "my_dataset")
    .load()
    .select("id", "name", "age");
```

This leverages Lance's column pushdown capabilities for better performance.

## Working with Different Data Types

### Primitive Types

Lance supports all standard Spark SQL data types:

```java
// Reading dataset with various primitive types
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "mixed_types")
    .load();

// Schema might look like:
// root
//  |-- id: long (nullable = true)
//  |-- name: string (nullable = true)
//  |-- age: integer (nullable = true)
//  |-- salary: double (nullable = true)
//  |-- is_active: boolean (nullable = true)
//  |-- created_at: timestamp (nullable = true)
```

### Complex Types

Lance also supports complex nested types:

```java
// Reading dataset with nested structures
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "nested_data")
    .load();

// Access nested fields
df.select(
    col("user.id"),
    col("user.profile.name"),
    col("addresses").getItem(0).getField("street")
).show();
```

### Vector Data

Lance excels at handling vector embeddings:

```java
// Reading dataset with vector embeddings
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "embeddings")
    .load();

// Vector columns are represented as arrays
df.select("id", "text", "embedding").show();
```

## Filtering Data

### Basic Filters

Apply filters to reduce the amount of data read:

```java
Dataset<Row> filtered = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "users")
    .load()
    .filter(col("age").gt(25));
```

### Complex Filters

Combine multiple filter conditions:

```java
Dataset<Row> filtered = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "users")
    .load()
    .filter(
        col("age").between(25, 65)
        .and(col("department").equalTo("Engineering"))
        .and(col("is_active").equalTo(true))
    );
```

### Filter Pushdown

The connector automatically pushes filters down to the Lance layer for optimal performance:

```java
// This filter is applied at the Lance level, not in Spark
Dataset<Row> efficient = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "large_dataset")
    .load()
    .filter(col("timestamp").gt("2023-01-01"));
```

## Reading from Different Storage Systems

### Local Filesystem

```java
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/local/path/to/lance/database")
    .option("dataset", "my_dataset")
    .load();
```

### Cloud Storage

#### Amazon S3

```java
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "s3://my-bucket/lance-database/")
    .option("dataset", "my_dataset")
    .load();
```

#### Google Cloud Storage

```java
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "gs://my-bucket/lance-database/")
    .option("dataset", "my_dataset")
    .load();
```

#### Azure Blob Storage

```java
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "abfss://container@account.dfs.core.windows.net/lance-database/")
    .option("dataset", "my_dataset")
    .load();
```

## Performance Optimization

### Partition Pruning

Lance automatically handles partition pruning when filters are applied:

```java
// Only reads relevant partitions
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "partitioned_data")
    .load()
    .filter(col("year").equalTo(2023));
```

### Column Pruning

Only read the columns you need:

```java
// More efficient - only reads specified columns
Dataset<Row> efficient = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "wide_table")
    .load()
    .select("id", "name", "important_metric");
```

### Batch Size Tuning

For large datasets, you can tune the batch size:

```java
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "large_dataset")
    .option("batchSize", "10000")  // Adjust based on your data
    .load();
```

## Working with Multiple Datasets

### Reading Multiple Datasets

```java
// Read multiple datasets and combine them
Dataset<Row> dataset1 = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "dataset1")
    .load();

Dataset<Row> dataset2 = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "dataset2")
    .load();

Dataset<Row> combined = dataset1.union(dataset2);
```

### Cross-Database Queries

```java
// Read from different Lance databases
Dataset<Row> db1Data = spark.read()
    .format("lance")
    .option("db", "/path/to/database1")
    .option("dataset", "users")
    .load();

Dataset<Row> db2Data = spark.read()
    .format("lance")
    .option("db", "/path/to/database2")
    .option("dataset", "orders")
    .load();

Dataset<Row> joined = db1Data.join(db2Data, "user_id");
```

## Error Handling

### Common Errors and Solutions

#### Dataset Not Found

```java
try {
    Dataset<Row> df = spark.read()
        .format("lance")
        .option("db", "/path/to/database")
        .option("dataset", "nonexistent_dataset")
        .load();
} catch (Exception e) {
    // Handle dataset not found error
    System.err.println("Dataset not found: " + e.getMessage());
}
```

#### Permission Errors

```java
// Ensure proper permissions for cloud storage
spark.conf().set("fs.s3a.access.key", "your-access-key");
spark.conf().set("fs.s3a.secret.key", "your-secret-key");

Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "s3://my-bucket/lance-database/")
    .option("dataset", "my_dataset")
    .load();
```

## Best Practices

### 1. Use Column Selection

Always select only the columns you need:

```java
// Good
Dataset<Row> efficient = df.select("id", "name", "age");

// Avoid
Dataset<Row> inefficient = df.select("*");
```

### 2. Apply Filters Early

Apply filters as early as possible to leverage pushdown:

```java
// Good - filter pushed down to Lance
Dataset<Row> filtered = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "large_table")
    .load()
    .filter(col("active").equalTo(true));

// Less efficient - filter applied in Spark
Dataset<Row> allData = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "large_table")
    .load();
Dataset<Row> laterFiltered = allData.filter(col("active").equalTo(true));
```

### 3. Cache Frequently Used Data

```java
Dataset<Row> frequentlyUsed = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "reference_data")
    .load()
    .cache();

// Use the cached dataset multiple times
Dataset<Row> result1 = frequentlyUsed.filter(col("type").equalTo("A"));
Dataset<Row> result2 = frequentlyUsed.filter(col("type").equalTo("B"));
```

## Next Steps

- [Configuration](configuration.md) - Learn about advanced configuration options
- [Performance Tuning](performance.md) - Optimize your queries for better performance
- [Examples](examples.md) - See more comprehensive examples 