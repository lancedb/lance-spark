# Lance-Spark Docker Setup

This Docker setup provides a complete Apache Spark environment with Lance format support, similar to the docker-spark-iceberg project but configured for Lance datasets.

## Features

- Apache Spark 3.5.6 with Lance-Spark connector
- Jupyter Notebook with PySpark
- MinIO for S3-compatible object storage
- Pre-configured Lance catalog with directory namespace
- Example notebooks demonstrating Lance functionality
- Spark UI, History Server, and Thrift Server

## Prerequisites

- Docker and Docker Compose installed
- Java 17 (for building the project)
- Maven (provided via `./mvnw` wrapper)

## Quick Start

### 1. Build the Lance-Spark Project

First, ensure you have Java 17 active and build the project:

```bash
./mvnw clean package -DskipTests
```

### 2. Start the Docker Environment

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

## Accessing Services

Once the containers are running, you can access:

- **Jupyter Notebook**: http://localhost:8888 (no password required)
- **Spark Master UI**: http://localhost:8080
- **Spark Application UI**: http://localhost:4040 (when a job is running)
- **MinIO Console**: http://localhost:9001 (admin/password)
- **Thrift Server**: localhost:10000 (JDBC/ODBC)

## Using Lance with Spark

### Via Jupyter Notebook

1. Open Jupyter at http://localhost:8888
2. Navigate to the example notebook: `Lance - Getting Started.ipynb`
3. Run through the examples to learn Lance-Spark functionality

### Via Spark Shell

```bash
# Enter the container
docker exec -it spark-lance bash

# Start PySpark shell
pyspark

# Or Spark SQL shell
spark-sql
```

### Via spark-submit

```bash
docker exec -it spark-lance spark-submit \
  --conf spark.sql.catalog.lance=com.lancedb.lance.spark.LanceNamespaceSparkCatalog \
  --conf spark.sql.catalog.lance.impl=dir \
  --conf spark.sql.catalog.lance.root=/home/lance/warehouse \
  your-application.py
```

## Configuration

### Directory Namespace (Default)

The default configuration uses the directory namespace with local storage:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("lance-example") \
    .config("spark.sql.catalog.lance", "com.lancedb.lance.spark.LanceNamespaceSparkCatalog") \
    .config("spark.sql.catalog.lance.impl", "dir") \
    .config("spark.sql.catalog.lance.root", "/home/lance/warehouse") \
    .getOrCreate()
```

### S3/MinIO Storage

To use MinIO (S3-compatible) storage:

```python
# Write to S3
df.write.format("lance").save("s3a://lance-warehouse/dataset.lance")

# Read from S3
spark.read.format("lance").load("s3a://lance-warehouse/dataset.lance")
```

### REST Namespace (LanceDB Cloud)

To use LanceDB Cloud, modify `docker/spark-defaults.conf`:

```properties
spark.sql.catalog.lance_cloud           com.lancedb.lance.spark.LanceNamespaceSparkCatalog
spark.sql.catalog.lance_cloud.impl      rest
spark.sql.catalog.lance_cloud.headers.x-api-key    your-api-key
spark.sql.catalog.lance_cloud.headers.x-lancedb-database    your-database
spark.sql.catalog.lance_cloud.uri       https://your-database.us-east-1.api.lancedb.com
```

## Example Usage

### Create and Query Lance Tables

```python
# Create a DataFrame
df = spark.range(100).selectExpr("id", "id * 2 as value")

# Write to Lance format
df.write.format("lance").save("/home/lance/warehouse/test.lance")

# Create a table in the catalog
df.writeTo("lance.default.test_table").using("lance").createOrReplace()

# Query using SQL
spark.sql("SELECT * FROM lance.default.test_table WHERE id > 50").show()
```

### Append Data

```python
# Append new data to existing table
new_df = spark.range(100, 200).selectExpr("id", "id * 2 as value")
new_df.writeTo("lance.default.test_table").append()
```

## Volumes and Persistence

The following directories are mounted as volumes:

- `./warehouse`: Lance datasets (persisted)
- `./docker/notebooks`: Jupyter notebooks
- `./lance-spark-bundle-3.5_2.12/target`: JAR files

## Troubleshooting

### Container won't start

Check if ports are already in use:
```bash
lsof -i :8888  # Jupyter
lsof -i :8080  # Spark Master UI
lsof -i :9000  # MinIO
```

### Lance catalog not available

Verify the JAR is properly copied:
```bash
docker exec spark-lance ls -la /opt/spark/jars/ | grep lance
```

### Memory issues

Adjust Spark memory settings in `docker-compose.yml`:
```yaml
environment:
  - SPARK_DRIVER_MEMORY=2g
  - SPARK_EXECUTOR_MEMORY=2g
```

## Development

To modify the Lance-Spark connector:

1. Make changes to the source code
2. Rebuild: `./mvnw clean package -DskipTests`
3. Copy the new JAR to docker directory
4. Rebuild Docker image: `docker-compose build`
5. Restart containers: `docker-compose up -d`

## Advanced Configuration

### Custom Namespace Implementation

You can configure different namespace implementations by modifying `spark.sql.catalog.lance.impl`:

- `dir`: Directory namespace (default)
- `rest`: REST namespace (LanceDB Cloud)
- `hive`: Hive metastore namespace
- `glue`: AWS Glue catalog namespace
- Or provide a full Java class name for custom implementations

### Performance Tuning

Optimize Lance performance with these settings:

```properties
# Enable filter push-down (default: true)
spark.sql.catalog.lance.pushDownFilters=true

# Adjust batch size for writing
spark.sql.catalog.lance.write.batch.size=10000

# Enable adaptive query execution
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
```

## License

This Docker setup follows the Apache License 2.0, consistent with the Lance-Spark project.

## Support

For issues or questions:
- Lance-Spark GitHub: https://github.com/lancedb/lance-spark
- LanceDB Documentation: https://lancedb.github.io/lance-spark