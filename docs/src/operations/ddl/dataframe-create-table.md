# DataFrame Create Table

Create Lance tables from DataFrames using the DataSource V2 API.

## Basic DataFrame Creation

=== "Python"
    ```python
    # Create DataFrame
    data = [
    (1, "Alice", "alice@example.com"),
    (2, "Bob", "bob@example.com"),
    (3, "Charlie", "charlie@example.com")
    ]
    df = spark.createDataFrame(data, ["id", "name", "email"])
    
    # Write as new table using catalog
    df.writeTo("users").create()
    ```

=== "Scala"
    ```scala
    import spark.implicits._
    
    // Create DataFrame
    val data = Seq(
        (1, "Alice", "alice@example.com"),
        (2, "Bob", "bob@example.com"),
        (3, "Charlie", "charlie@example.com")
    )
    val df = data.toDF("id", "name", "email")
    
    // Write as new table using catalog
    df.writeTo("users").create()
    ```

=== "Java"
    ```java
    import org.apache.spark.sql.types.*;
    import org.apache.spark.sql.Row;
    import org.apache.spark.sql.RowFactory;
    
    // Create DataFrame
    List<Row> data = Arrays.asList(
        RowFactory.create(1L, "Alice", "alice@example.com"),
        RowFactory.create(2L, "Bob", "bob@example.com"),
        RowFactory.create(3L, "Charlie", "charlie@example.com")
    );
    
    StructType schema = new StructType(new StructField[]{
        new StructField("id", DataTypes.LongType, false, Metadata.empty()),
        new StructField("name", DataTypes.StringType, true, Metadata.empty()),
        new StructField("email", DataTypes.StringType, true, Metadata.empty())
    });
    
    Dataset<Row> df = spark.createDataFrame(data, schema);
    
    // Write as new table using catalog
    df.writeTo("users").create();
    ```

## Creating Tables with Vector Columns

Lance supports vector columns for machine learning workloads. You can create DataFrames with vector columns by adding metadata to the schema field definition.

### Supported Types

- **Element Types**: `FloatType` (float32), `DoubleType` (float64)
- **Array Requirements**:
  - Must have `containsNull = false`
  - Column must be non-nullable
  - All arrays must have exactly the specified dimension

### Examples

=== "Python"
    ```python
    from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, FloatType
    from pyspark.sql.types import Metadata
    
    # Create schema with vector column
    vector_metadata = {"arrow.fixed-size-list.size": 128}
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("embeddings", ArrayType(FloatType(), False), False, vector_metadata)
    ])
    
    # Create DataFrame with vector data
    import numpy as np
    data = [(i, np.random.rand(128).astype(np.float32).tolist()) for i in range(100)]
    df = spark.createDataFrame(data, schema)
    
    # Write to Lance format
    df.writeTo("vectors_table").create()
    ```

=== "Scala"
    ```scala
    import org.apache.spark.sql.types._
    
    // Create metadata for vector column
    val vectorMetadata = new MetadataBuilder()
      .putLong("arrow.fixed-size-list.size", 128)
      .build()
    
    // Create schema with vector column
    val schema = StructType(Array(
      StructField("id", IntegerType, false),
      StructField("embeddings", ArrayType(FloatType, false), false, vectorMetadata)
    ))
    
    // Create DataFrame with vector data
    import scala.util.Random
    val data = (0 until 100).map { i =>
      (i, Array.fill(128)(Random.nextFloat()))
    }
    val df = spark.createDataFrame(data).toDF("id", "embeddings")
    
    // Write to Lance format
    df.writeTo("vectors_table").create()
    ```

=== "Java"
    ```java
    import org.apache.spark.sql.types.*;
    
    // Create metadata for vector column
    Metadata vectorMetadata = new MetadataBuilder()
        .putLong("arrow.fixed-size-list.size", 128)
        .build();
    
    // Create schema with vector column
    StructType schema = new StructType(new StructField[] {
        DataTypes.createStructField("id", DataTypes.IntegerType, false),
        DataTypes.createStructField("embeddings", 
            DataTypes.createArrayType(DataTypes.FloatType, false),
            false, vectorMetadata)
    });
    
    // Create DataFrame with vector data
    List<Row> rows = new ArrayList<>();
    Random random = new Random();
    for (int i = 0; i < 100; i++) {
        float[] vector = new float[128];
        for (int j = 0; j < 128; j++) {
            vector[j] = random.nextFloat();
        }
        rows.add(RowFactory.create(i, vector));
    }
    Dataset<Row> df = spark.createDataFrame(rows, schema);
    
    // Write to Lance format
    df.writeTo("vectors_table").create();
    ```

### Creating Multiple Vector Columns

You can create DataFrames with multiple vector columns of different dimensions:

=== "Python"
    ```python
    # Create schema with multiple vector columns
    text_metadata = {"arrow.fixed-size-list.size": 384}
    image_metadata = {"arrow.fixed-size-list.size": 512}
    
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("text_embeddings", ArrayType(FloatType(), False), False, text_metadata),
        StructField("image_embeddings", ArrayType(DoubleType(), False), False, image_metadata)
    ])
    
    # Create DataFrame with multiple vector columns
    data = [
        (i, 
         np.random.rand(384).astype(np.float32).tolist(),
         np.random.rand(512).tolist())
        for i in range(100)
    ]
    df = spark.createDataFrame(data, schema)
    
    # Write to Lance format
    df.writeTo("multi_vectors_table").create()
    ```

### Vector Indexing

After creating tables with vector columns, you can create vector indexes using Lance Python API:

```python
import lance

# Open the dataset
ds = lance.dataset("/path/to/vectors_table.lance")

# Create a vector index on the embeddings column
ds.create_index(
    "embeddings",
    index_type="IVF_PQ",
    num_partitions=256,
    num_sub_vectors=16
)

# Perform similarity search
query_vector = np.random.rand(128).astype(np.float32)
results = ds.to_table(
    nearest={"column": "embeddings", "q": query_vector, "k": 10}
).to_pandas()
```