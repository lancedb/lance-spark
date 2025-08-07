# Vector Column Support in Lance-Spark

This document describes the vector column support in Lance-Spark, which enables writing Spark DataFrame ArrayType columns as Arrow FixedSizeList for ML workloads and Lance vector indexing.

## Overview

Lance format uses Arrow FixedSizeList format to define vectors (fixed-dimension arrays of floats). This feature allows users to write Spark DataFrames with array columns as FixedSizeList in Lance format, enabling:

- Efficient storage of vector embeddings
- Vector index creation for similarity search
- Compatibility with Lance's ML features

## Usage

To mark an array column as a vector, add metadata to the StructField:

```scala
import org.apache.spark.sql.types._

val vectorMetadata = new MetadataBuilder()
  .putLong("arrow.FixedSizeList.size", 128)  // Vector dimension
  .build()

val schema = new StructType(Array(
  StructField("id", IntegerType, false),
  StructField("embeddings", ArrayType(FloatType, false), false, vectorMetadata)
))
```

## Supported Types

- **Element Types**: `FloatType` (float32), `DoubleType` (float64)
- **Array Requirements**:
  - Must have `containsNull = false`
  - Column must be non-nullable
  - All arrays must have exactly the specified dimension

## Example

```java
// Create schema with vector column
Metadata vectorMetadata = new MetadataBuilder()
    .putLong("arrow.FixedSizeList.size", 128)
    .build();

StructType schema = new StructType(new StructField[] {
    DataTypes.createStructField("id", DataTypes.IntegerType, false),
    DataTypes.createStructField("embeddings", 
        DataTypes.createArrayType(DataTypes.FloatType, false),
        false, vectorMetadata)
});

// Create DataFrame with vector data
List<Row> rows = new ArrayList<>();
for (int i = 0; i < 100; i++) {
    float[] vector = new float[128];
    // ... populate vector
    rows.add(RowFactory.create(i, vector));
}

Dataset<Row> df = spark.createDataFrame(rows, schema);

// Write to Lance format
df.write()
    .format("lance")
    .mode("overwrite")
    .save("/path/to/dataset.lance");
```

## Implementation Details

The implementation uses a custom `LanceArrowWriter` that:
1. Detects array fields with `arrow.FixedSizeList.size` metadata
2. Creates Arrow FixedSizeList fields in the schema
3. Directly converts Spark array data to FixedSizeList during writing
4. Validates dimension consistency at write time

## Error Handling

The writer will throw an error if:
- Array dimension doesn't match the specified size
- Null values are encountered in vector columns
- Unsupported element types are used (only float/double supported)