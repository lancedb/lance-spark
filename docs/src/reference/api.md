# API Reference

This page provides a comprehensive reference for the Apache Spark Connector for Lance API.

## DataSource API

### LanceDataSource

The main entry point for reading Lance datasets in Spark.

#### Usage

```java
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "dataset_name")
    .load();
```

#### Options

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `db` | String | Yes | - | Path to the Lance database directory |
| `dataset` | String | Yes | - | Name of the Lance dataset |
| `batchSize` | Integer | No | 8192 | Number of rows to read per batch |
| `maxMemoryPerBatch` | String | No | "32MB" | Maximum memory per batch |
| `prefetchBatches` | Integer | No | 1 | Number of batches to prefetch |
| `connectionTimeout` | Integer | No | 30000 | Connection timeout in milliseconds |
| `readTimeout` | Integer | No | 60000 | Read timeout in milliseconds |

## Configuration Options

### Required Configuration

#### Database Path (`db`)

Specifies the path to the Lance database directory.

**Type**: String  
**Required**: Yes  
**Examples**:
- Local filesystem: `/data/lance_db`
- Amazon S3: `s3://bucket/path/to/db`
- Google Cloud Storage: `gs://bucket/path/to/db`
- Azure Blob Storage: `abfss://container@account.dfs.core.windows.net/path/to/db`

#### Dataset Name (`dataset`)

Specifies the name of the Lance dataset within the database.

**Type**: String  
**Required**: Yes  
**Example**: `users`, `transactions`, `embeddings`

### Performance Configuration

#### Batch Size (`batchSize`)

Controls the number of rows read in each batch from Lance.

**Type**: Integer  
**Default**: 8192  
**Range**: 1000 - 50000  
**Recommendations**:
- Small datasets or low memory: 1000-5000
- Default balanced performance: 8192
- Large datasets with high memory: 15000-25000

```java
.option("batchSize", "10000")
```

#### Max Memory Per Batch (`maxMemoryPerBatch`)

Sets the maximum memory limit for each batch.

**Type**: String (with units)  
**Default**: "32MB"  
**Units**: KB, MB, GB  
**Examples**: "16MB", "64MB", "128MB"

```java
.option("maxMemoryPerBatch", "64MB")
```

#### Prefetch Batches (`prefetchBatches`)

Number of batches to prefetch for better performance.

**Type**: Integer  
**Default**: 1  
**Range**: 1-5  
**Use Cases**:
- Network storage: 2-3
- Cloud storage: 3-5
- Local storage: 1-2

```java
.option("prefetchBatches", "2")
```

### Network Configuration

#### Connection Timeout (`connectionTimeout`)

Timeout for establishing connections to storage systems.

**Type**: Integer (milliseconds)  
**Default**: 30000 (30 seconds)  
**Range**: 5000-300000

```java
.option("connectionTimeout", "60000")  // 60 seconds
```

#### Read Timeout (`readTimeout`)

Timeout for read operations from storage systems.

**Type**: Integer (milliseconds)  
**Default**: 60000 (60 seconds)  
**Range**: 10000-600000

```java
.option("readTimeout", "120000")  // 2 minutes
```

### Cloud Storage Configuration

#### Amazon S3

```java
// S3 specific options
.option("s3.region", "us-west-2")
.option("s3.endpoint", "s3.amazonaws.com")
.option("s3.pathStyleAccess", "false")
```

#### Google Cloud Storage

```java
// GCS specific options
.option("gcs.projectId", "my-project")
.option("gcs.keyFile", "/path/to/keyfile.json")
```

#### Azure Blob Storage

```java
// Azure specific options
.option("azure.accountName", "myaccount")
.option("azure.containerName", "mycontainer")
```

## Data Types

### Supported Spark SQL Types

The connector supports all standard Spark SQL data types:

| Lance Type | Spark SQL Type | Java Type | Description |
|------------|----------------|-----------|-------------|
| Boolean | BooleanType | Boolean | True/false values |
| Int8 | ByteType | Byte | 8-bit signed integer |
| Int16 | ShortType | Short | 16-bit signed integer |
| Int32 | IntegerType | Integer | 32-bit signed integer |
| Int64 | LongType | Long | 64-bit signed integer |
| Float32 | FloatType | Float | 32-bit floating point |
| Float64 | DoubleType | Double | 64-bit floating point |
| String | StringType | String | UTF-8 encoded strings |
| Binary | BinaryType | byte[] | Binary data |
| Date | DateType | Date | Date values |
| Timestamp | TimestampType | Timestamp | Timestamp values |

### Complex Types

| Lance Type | Spark SQL Type | Description |
|------------|----------------|-------------|
| List | ArrayType | Arrays of values |
| Struct | StructType | Nested structures |
| Map | MapType | Key-value mappings |

### Vector Types

Lance provides special support for vector embeddings:

```java
// Vector columns are represented as ArrayType(DoubleType)
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/db")
    .option("dataset", "embeddings")
    .load();

// Access vector data
df.select("id", "text", "embedding").show();
```

## Error Handling

### Common Exceptions

#### DatasetNotFoundException

Thrown when the specified dataset cannot be found.

```java
try {
    Dataset<Row> df = spark.read()
        .format("lance")
        .option("db", "/path/to/db")
        .option("dataset", "nonexistent")
        .load();
} catch (DatasetNotFoundException e) {
    System.err.println("Dataset not found: " + e.getMessage());
}
```

#### InvalidConfigurationException

Thrown when configuration options are invalid.

```java
try {
    Dataset<Row> df = spark.read()
        .format("lance")
        .option("db", "")  // Invalid empty path
        .option("dataset", "test")
        .load();
} catch (InvalidConfigurationException e) {
    System.err.println("Invalid configuration: " + e.getMessage());
}
```

#### ConnectionTimeoutException

Thrown when connection to storage times out.

```java
try {
    Dataset<Row> df = spark.read()
        .format("lance")
        .option("db", "s3://slow-bucket/db")
        .option("dataset", "test")
        .option("connectionTimeout", "5000")  // Very short timeout
        .load();
} catch (ConnectionTimeoutException e) {
    System.err.println("Connection timeout: " + e.getMessage());
}
```

## Performance Optimization

### Column Pushdown

The connector automatically pushes column selections to Lance:

```java
// Only reads specified columns from Lance
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/db")
    .option("dataset", "wide_table")
    .load()
    .select("id", "name", "age");  // Column pushdown applied
```

### Filter Pushdown

Filters are pushed down to the Lance layer:

```java
// Filter applied at Lance level
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/db")
    .option("dataset", "users")
    .load()
    .filter(col("age").gt(25));  // Filter pushdown applied
```

### Supported Filter Operations

| Operation | Spark SQL | Pushed Down |
|-----------|-----------|-------------|
| Equality | `col("x").equalTo(value)` | ✅ Yes |
| Inequality | `col("x").notEqual(value)` | ✅ Yes |
| Greater than | `col("x").gt(value)` | ✅ Yes |
| Greater than or equal | `col("x").geq(value)` | ✅ Yes |
| Less than | `col("x").lt(value)` | ✅ Yes |
| Less than or equal | `col("x").leq(value)` | ✅ Yes |
| Between | `col("x").between(a, b)` | ✅ Yes |
| In | `col("x").isin(values)` | ✅ Yes |
| Is null | `col("x").isNull()` | ✅ Yes |
| Is not null | `col("x").isNotNull()` | ✅ Yes |
| And | `filter1.and(filter2)` | ✅ Yes |
| Or | `filter1.or(filter2)` | ✅ Yes |
| Like | `col("x").like(pattern)` | ❌ No |
| Regex | `col("x").rlike(pattern)` | ❌ No |

## Integration Examples

### Spark SQL

```sql
-- Register as temporary view
CREATE OR REPLACE TEMPORARY VIEW lance_data
USING lance
OPTIONS (
  db '/path/to/database',
  dataset 'my_dataset'
);

-- Query using SQL
SELECT id, name, age
FROM lance_data
WHERE age > 25
ORDER BY age DESC;
```

### DataFrame API

```java
// Read and transform
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "users")
    .load();

// Apply transformations
Dataset<Row> result = df
    .filter(col("active").equalTo(true))
    .select("id", "name", "email")
    .orderBy(col("name"));

// Show results
result.show();
```

### Joining with Other Sources

```java
// Read Lance data
Dataset<Row> lanceData = spark.read()
    .format("lance")
    .option("db", "/path/to/lance/db")
    .option("dataset", "users")
    .load();

// Read Parquet data
Dataset<Row> parquetData = spark.read()
    .parquet("/path/to/parquet/orders");

// Join datasets
Dataset<Row> joined = lanceData
    .join(parquetData, "user_id")
    .select("users.name", "orders.amount", "orders.date");
```

## Version Compatibility

### Spark Version Support

| Spark Version | Connector Version | Status |
|---------------|-------------------|--------|
| 3.4.x | 0.0.1+ | ✅ Supported |
| 3.5.x | 0.0.1+ | ✅ Supported |

### Scala Version Support

| Scala Version | Connector Version | Status |
|---------------|-------------------|--------|
| 2.12.x | 0.0.1+ | ✅ Supported |
| 2.13.x | Future | 🚧 Planned |

## Next Steps

- [Configuration Guide](../user-guide/configuration.md) - Detailed configuration options
- [Performance Tuning](../user-guide/performance.md) - Optimize your queries
- [Troubleshooting](troubleshooting.md) - Common issues and solutions 