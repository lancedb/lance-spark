# Configuration

This guide covers all configuration options available for the Apache Spark Connector for Lance.

## Basic Configuration

### Required Options

These options must be specified when reading Lance datasets:

| Option | Type | Description | Example |
|--------|------|-------------|---------|
| `db` | String | Path to the Lance database directory | `/data/lance_db` |
| `dataset` | String | Name of the Lance dataset | `users` |

### Example

```java
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/lance/database")
    .option("dataset", "my_dataset")
    .load();
```

## Advanced Configuration Options

### Performance Tuning

#### Batch Size

Control the number of rows read in each batch:

```java
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "large_dataset")
    .option("batchSize", "10000")  // Default: 8192
    .load();
```

| Value | Use Case |
|-------|----------|
| 1000-5000 | Small datasets, low memory |
| 8192 | Default, balanced performance |
| 20000+ | Large datasets, high memory |

#### Memory Management

Configure memory usage for Arrow operations:

```java
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "my_dataset")
    .option("maxMemoryPerBatch", "64MB")  // Default: 32MB
    .load();
```

### Cloud Storage Configuration

#### Amazon S3

```java
// Configure S3 access
spark.conf().set("fs.s3a.access.key", "your-access-key");
spark.conf().set("fs.s3a.secret.key", "your-secret-key");
spark.conf().set("fs.s3a.endpoint", "s3.amazonaws.com");

Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "s3://my-bucket/lance-database/")
    .option("dataset", "my_dataset")
    .option("s3.region", "us-west-2")
    .load();
```

#### Google Cloud Storage

```java
// Configure GCS access
spark.conf().set("fs.gs.auth.service.account.enable", "true");
spark.conf().set("fs.gs.auth.service.account.json.keyfile", "/path/to/keyfile.json");

Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "gs://my-bucket/lance-database/")
    .option("dataset", "my_dataset")
    .load();
```

#### Azure Blob Storage

```java
// Configure Azure access
spark.conf().set("fs.azure.account.key.myaccount.blob.core.windows.net", "your-account-key");

Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "abfss://container@myaccount.dfs.core.windows.net/lance-database/")
    .option("dataset", "my_dataset")
    .load();
```

## Spark Configuration

### Executor Configuration

Recommended Spark configuration for Lance workloads:

```bash
# Memory configuration
spark.executor.memory=4g
spark.executor.memoryFraction=0.8
spark.executor.cores=4

# Enable adaptive query execution
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true

# Arrow configuration
spark.sql.execution.arrow.pyspark.enabled=true
spark.sql.execution.arrow.maxRecordsPerBatch=10000
```

### Driver Configuration

```bash
# Driver memory for large schemas
spark.driver.memory=2g
spark.driver.maxResultSize=1g

# Network timeouts for cloud storage
spark.network.timeout=600s
spark.sql.broadcastTimeout=600
```

## Configuration Examples

### High-Performance Setup

For maximum performance with large datasets:

```java
SparkSession spark = SparkSession.builder()
    .appName("lance-high-performance")
    .config("spark.executor.memory", "8g")
    .config("spark.executor.cores", "8")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .getOrCreate();

Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "large_dataset")
    .option("batchSize", "20000")
    .option("maxMemoryPerBatch", "128MB")
    .load();
```

### Memory-Constrained Setup

For environments with limited memory:

```java
SparkSession spark = SparkSession.builder()
    .appName("lance-memory-efficient")
    .config("spark.executor.memory", "2g")
    .config("spark.executor.memoryFraction", "0.6")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate();

Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "my_dataset")
    .option("batchSize", "5000")
    .option("maxMemoryPerBatch", "16MB")
    .load();
```

### Cloud-Optimized Setup

For cloud storage with high latency:

```java
SparkSession spark = SparkSession.builder()
    .appName("lance-cloud-optimized")
    .config("spark.network.timeout", "600s")
    .config("spark.sql.broadcastTimeout", "600")
    .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
    .config("spark.hadoop.fs.s3a.socket.recv.buffer", "65536")
    .getOrCreate();

Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "s3://my-bucket/lance-database/")
    .option("dataset", "my_dataset")
    .option("batchSize", "15000")
    .option("prefetchBatches", "3")
    .load();
```

## Environment Variables

### Lance-Specific Variables

```bash
# Set Lance log level
export LANCE_LOG_LEVEL=INFO

# Configure Lance cache directory
export LANCE_CACHE_DIR=/tmp/lance_cache

# Set maximum number of open files
export LANCE_MAX_OPEN_FILES=1000
```

### JVM Configuration

```bash
# Increase heap size for large datasets
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=8g

# Optimize garbage collection
export SPARK_DRIVER_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200"
export SPARK_EXECUTOR_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200"
```

## Configuration Best Practices

### 1. Start with Defaults

Begin with default settings and adjust based on your workload:

```java
Dataset<Row> df = spark.read()
    .format("lance")
    .option("db", "/path/to/database")
    .option("dataset", "my_dataset")
    .load();
```

### 2. Monitor Performance

Use Spark UI to monitor:
- Task execution times
- Memory usage
- Data skew
- Shuffle operations

### 3. Tune Batch Size

Adjust batch size based on your data characteristics:

```java
// For wide tables (many columns)
.option("batchSize", "5000")

// For narrow tables (few columns)
.option("batchSize", "20000")

// For memory-constrained environments
.option("batchSize", "2000")
```

### 4. Configure for Your Storage

#### Local Storage
```java
// Optimize for local SSD
.option("batchSize", "15000")
.option("maxMemoryPerBatch", "64MB")
```

#### Network Storage
```java
// Optimize for network latency
.option("batchSize", "10000")
.option("prefetchBatches", "2")
```

#### Cloud Storage
```java
// Optimize for cloud latency
.option("batchSize", "20000")
.option("prefetchBatches", "3")
.option("connectionTimeout", "60000")
```

## Troubleshooting Configuration Issues

### Common Problems

#### Out of Memory Errors

```java
// Reduce batch size and memory per batch
.option("batchSize", "5000")
.option("maxMemoryPerBatch", "16MB")
```

#### Slow Performance

```java
// Increase batch size and enable prefetching
.option("batchSize", "15000")
.option("prefetchBatches", "2")
```

#### Connection Timeouts

```java
// Increase timeout values
spark.conf().set("spark.network.timeout", "600s");
spark.conf().set("spark.sql.broadcastTimeout", "600");
```

### Debugging Configuration

Enable debug logging to troubleshoot configuration issues:

```java
spark.sparkContext().setLogLevel("DEBUG");

// Or set specific logger levels
Logger.getLogger("com.lancedb.lance.spark").setLevel(Level.DEBUG);
```

## Configuration Reference

### Complete Option List

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `db` | String | Required | Database path |
| `dataset` | String | Required | Dataset name |
| `batchSize` | Integer | 8192 | Rows per batch |
| `maxMemoryPerBatch` | String | "32MB" | Memory limit per batch |
| `prefetchBatches` | Integer | 1 | Number of batches to prefetch |
| `connectionTimeout` | Integer | 30000 | Connection timeout (ms) |
| `readTimeout` | Integer | 60000 | Read timeout (ms) |

## Next Steps

- [Performance Tuning](performance.md) - Optimize your Lance queries
- [Examples](examples.md) - See configuration examples in action
- [Troubleshooting](../reference/troubleshooting.md) - Solve common configuration issues 