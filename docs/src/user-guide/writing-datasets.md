# Writing Lance Datasets

This guide covers everything you need to know about writing data to Lance datasets using the Apache Spark Connector for Lance.

## Basic Writing

### Simple Dataset Writing

The most basic way to write data to a Lance dataset:

```java
Dataset<Row> df = // your DataFrame
df.write()
    .format("lance")
    .option("dataset_uri", "/path/to/lance/database/my_dataset")
    .save();
```

### Required Options

| Option | Description | Example |
|--------|-------------|---------|
| `dataset_uri` | Full URI path to the Lance dataset | `/data/lance_db/users` |

Alternatively, you can specify the path directly in the `save()` method:

```java
df.write()
    .format("lance")
    .save("/path/to/lance/database/my_dataset");
```

## Write Modes

### Default Write (ErrorIfExists)

By default, writing to an existing dataset will throw an error:

```java
// First write - succeeds
testData.write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/users")
    .save();

// Second write - throws TableAlreadyExistsException
testData.write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/users")
    .save(); // This will fail
```

### Append Mode

Add new data to an existing dataset:

```java
// Create initial dataset
testData.write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/users")
    .save();

// Append more data
moreData.write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/users")
    .mode("append")
    .save();
```

**Note**: Append mode requires the dataset to already exist. If it doesn't exist, a `NoSuchTableException` will be thrown.

### Overwrite Mode

Replace the entire dataset with new data:

```java
// Create initial dataset
initialData.write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/users")
    .save();

// Completely replace the dataset
newData.write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/users")
    .mode("overwrite")
    .save();
```

### Mode Combinations

You can combine different write modes in sequence:

```java
// Initial write
testData.write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/users")
    .save();

// Overwrite
testData.write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/users")
    .mode("overwrite")
    .save();

// Then append
testData.write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/users")
    .mode("append")
    .save();
```

## Creating DataFrames for Writing

### From Collections

```java
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.RowFactory;

// Define schema
StructType schema = new StructType(new StructField[]{
    DataTypes.createStructField("id", DataTypes.IntegerType, false),
    DataTypes.createStructField("name", DataTypes.StringType, false),
    DataTypes.createStructField("age", DataTypes.IntegerType, true),
    DataTypes.createStructField("salary", DataTypes.DoubleType, true)
});

// Create rows
Row row1 = RowFactory.create(1, "Alice", 30, 75000.0);
Row row2 = RowFactory.create(2, "Bob", 25, 65000.0);
List<Row> data = Arrays.asList(row1, row2);

// Create DataFrame
Dataset<Row> df = spark.createDataFrame(data, schema);

// Write to Lance
df.write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/employees")
    .save();
```

### From SQL Queries

```java
// Create temporary view from existing data
existingData.createOrReplaceTempView("temp_data");

// Transform and write
spark.sql("SELECT id, UPPER(name) as name, age * 2 as double_age FROM temp_data")
    .write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/transformed_data")
    .save();
```

### From Other Data Sources

```java
// Read from Parquet and write to Lance
Dataset<Row> parquetData = spark.read()
    .parquet("/path/to/input.parquet");

parquetData.write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/converted_data")
    .save();

// Read from JSON and write to Lance
Dataset<Row> jsonData = spark.read()
    .json("/path/to/input.json");

jsonData.write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/json_data")
    .save();
```

## Working with Different Data Types

### Primitive Types

Lance supports all standard Spark SQL data types:

```java
StructType schema = new StructType(new StructField[]{
    DataTypes.createStructField("id", DataTypes.LongType, false),
    DataTypes.createStructField("name", DataTypes.StringType, false),
    DataTypes.createStructField("age", DataTypes.IntegerType, true),
    DataTypes.createStructField("salary", DataTypes.DoubleType, true),
    DataTypes.createStructField("is_active", DataTypes.BooleanType, true),
    DataTypes.createStructField("hire_date", DataTypes.DateType, true),
    DataTypes.createStructField("last_login", DataTypes.TimestampType, true)
});

// Create and write data with various types
Dataset<Row> df = spark.createDataFrame(data, schema);
df.write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/mixed_types")
    .save();
```

### Complex Types

```java
// Array type
StructField arrayField = DataTypes.createStructField("tags", 
    DataTypes.createArrayType(DataTypes.StringType), true);

// Struct type
StructType addressType = new StructType(new StructField[]{
    DataTypes.createStructField("street", DataTypes.StringType, true),
    DataTypes.createStructField("city", DataTypes.StringType, true),
    DataTypes.createStructField("zipcode", DataTypes.StringType, true)
});
StructField structField = DataTypes.createStructField("address", addressType, true);

// Map type
StructField mapField = DataTypes.createStructField("metadata",
    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType), true);

StructType complexSchema = new StructType(new StructField[]{
    DataTypes.createStructField("id", DataTypes.IntegerType, false),
    DataTypes.createStructField("name", DataTypes.StringType, false),
    arrayField,
    structField,
    mapField
});
```

### Vector Data

Lance excels at handling vector embeddings:

```java
// Vector embeddings as arrays
StructType embeddingSchema = new StructType(new StructField[]{
    DataTypes.createStructField("id", DataTypes.IntegerType, false),
    DataTypes.createStructField("text", DataTypes.StringType, false),
    DataTypes.createStructField("embedding", 
        DataTypes.createArrayType(DataTypes.DoubleType), false)
});

// Create sample embedding data
Row embeddingRow = RowFactory.create(1, "sample text", 
    Arrays.asList(0.1, 0.2, 0.3, 0.4, 0.5));
Dataset<Row> embeddingData = spark.createDataFrame(
    Arrays.asList(embeddingRow), embeddingSchema);

embeddingData.write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/embeddings")
    .save();
```

## Writing to Different Storage Systems

### Local Filesystem

```java
df.write()
    .format("lance")
    .option("dataset_uri", "/local/path/to/lance/database/my_dataset")
    .save();
```

### Cloud Storage

#### Amazon S3

```java
// Configure S3 credentials
spark.conf().set("fs.s3a.access.key", "your-access-key");
spark.conf().set("fs.s3a.secret.key", "your-secret-key");

df.write()
    .format("lance")
    .option("dataset_uri", "s3://my-bucket/lance-database/my_dataset")
    .save();
```

#### Google Cloud Storage

```java
// Configure GCS credentials
spark.conf().set("fs.gs.auth.service.account.enable", "true");
spark.conf().set("fs.gs.auth.service.account.json.keyfile", "/path/to/keyfile.json");

df.write()
    .format("lance")
    .option("dataset_uri", "gs://my-bucket/lance-database/my_dataset")
    .save();
```

#### Azure Blob Storage

```java
// Configure Azure credentials
spark.conf().set("fs.azure.account.key.myaccount.blob.core.windows.net", "your-account-key");

df.write()
    .format("lance")
    .option("dataset_uri", "abfss://container@myaccount.dfs.core.windows.net/lance-database/my_dataset")
    .save();
```

## Performance Optimization

### Partitioning Data

Control the number of files created by partitioning your data:

```java
// Repartition before writing for optimal file sizes
df.repartition(4)
    .write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/partitioned_data")
    .save();
```

### Controlling File Size

Configure the maximum rows per file:

```java
SparkSession spark = SparkSession.builder()
    .appName("lance-writer")
    .config("spark.sql.catalog.lance.max_row_per_file", "10000")
    .getOrCreate();

df.write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/optimized_data")
    .save();
```

### Handling Empty Partitions

The connector automatically handles empty partitions:

```java
// Even with empty partitions, only non-empty files are created
df.repartition(10) // May create empty partitions
    .write()
    .format("lance")
    .option("dataset_uri", "/path/to/database/sparse_data")
    .save();
```

## Using Lance Catalog

### Catalog Configuration

Configure Spark to use the Lance catalog:

```java
SparkSession spark = SparkSession.builder()
    .appName("lance-catalog-example")
    .config("spark.sql.catalog.lance", "com.lancedb.lance.spark.LanceCatalog")
    .getOrCreate();
```

### SQL DDL Operations

#### CREATE TABLE

```sql
-- Create table from existing data
CREATE TABLE lance.`/path/to/database/users` AS 
SELECT * FROM temp_view;

-- Create or replace table
CREATE OR REPLACE TABLE lance.`/path/to/database/users` AS 
SELECT id, name, age FROM source_table;
```

#### DROP TABLE

```sql
-- Drop a Lance table
DROP TABLE lance.`/path/to/database/users`;
```

### Programmatic Catalog Operations

```java
// Create temporary view
df.createOrReplaceTempView("temp_data");

// Create table using SQL
spark.sql("CREATE OR REPLACE TABLE lance.`/path/to/database/my_table` AS SELECT * FROM temp_data");

// Drop table using SQL
spark.sql("DROP TABLE lance.`/path/to/database/my_table`");
```

## Error Handling

### Common Errors and Solutions

#### Table Already Exists

```java
try {
    df.write()
        .format("lance")
        .option("dataset_uri", "/path/to/existing/dataset")
        .save();
} catch (TableAlreadyExistsException e) {
    // Handle existing table - use append or overwrite mode
    df.write()
        .format("lance")
        .option("dataset_uri", "/path/to/existing/dataset")
        .mode("overwrite")
        .save();
}
```

#### Dataset Not Found (Append Mode)

```java
try {
    df.write()
        .format("lance")
        .option("dataset_uri", "/path/to/nonexistent/dataset")
        .mode("append")
        .save();
} catch (NoSuchTableException e) {
    // Create the dataset first
    df.write()
        .format("lance")
        .option("dataset_uri", "/path/to/nonexistent/dataset")
        .save();
}
```

#### Permission Errors

```java
// Ensure proper permissions for cloud storage
try {
    df.write()
        .format("lance")
        .option("dataset_uri", "s3://my-bucket/dataset")
        .save();
} catch (Exception e) {
    // Check credentials and permissions
    System.err.println("Write failed: " + e.getMessage());
}
```

## Best Practices

### 1. Choose Appropriate Write Modes

```java
// For initial data loading
df.write().format("lance").save("/path/to/dataset");

// For incremental updates
df.write().format("lance").mode("append").save("/path/to/dataset");

// For complete refresh
df.write().format("lance").mode("overwrite").save("/path/to/dataset");
```

### 2. Optimize Partitioning

```java
// For large datasets, partition appropriately
df.repartition(numCores * 2)
    .write()
    .format("lance")
    .save("/path/to/dataset");

// For small datasets, avoid over-partitioning
smallDf.coalesce(1)
    .write()
    .format("lance")
    .save("/path/to/dataset");
```

### 3. Handle Schema Evolution

```java
// Ensure consistent schemas when appending
Dataset<Row> newData = existingData.select("id", "name", "age"); // Match existing schema
newData.write()
    .format("lance")
    .mode("append")
    .save("/path/to/dataset");
```

### 4. Use Transactions for Consistency

```java
// Use overwrite mode for atomic updates
df.write()
    .format("lance")
    .mode("overwrite")
    .save("/path/to/dataset");
```

## Integration Examples

### ETL Pipeline

```java
// Extract
Dataset<Row> sourceData = spark.read()
    .jdbc("jdbc:postgresql://localhost/db", "source_table", props);

// Transform
Dataset<Row> transformedData = sourceData
    .filter(col("active").equalTo(true))
    .withColumn("processed_date", current_date())
    .select("id", "name", "email", "processed_date");

// Load
transformedData.write()
    .format("lance")
    .mode("overwrite")
    .save("/path/to/warehouse/users");
```

### Batch Processing

```java
// Process data in batches
Dataset<Row> batchData = spark.read()
    .option("batchSize", "10000")
    .parquet("/path/to/input/batch_*.parquet");

batchData.write()
    .format("lance")
    .mode("append")
    .save("/path/to/warehouse/processed_data");
```

### Data Migration

```java
// Migrate from Parquet to Lance
Dataset<Row> parquetData = spark.read()
    .parquet("/path/to/legacy/data/*.parquet");

parquetData.write()
    .format("lance")
    .mode("overwrite")
    .save("/path/to/new/lance/dataset");
```

## Next Steps

- [Reading Datasets](reading-datasets.md) - Learn how to read Lance datasets
- [Configuration](configuration.md) - Advanced configuration options
- [Performance Tuning](performance.md) - Optimize your write operations
- [Examples](examples.md) - More comprehensive examples 