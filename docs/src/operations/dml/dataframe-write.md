# DataFrame Write

Append data to existing Lance tables using DataFrames.

=== "Python"
    ```python
    # Create new data
    new_data = [
        (8, "Henry", "henry@example.com"),
        (9, "Ivy", "ivy@example.com")
    ]
    new_df = spark.createDataFrame(new_data, ["id", "name", "email"])
    
    # Append to existing table
    new_df.writeTo("users").append()
    
    # Alternative: use traditional write API with mode
    new_df.write.mode("append").saveAsTable("users")
    ```

=== "Scala"
    ```scala
    // Create new data
    val newData = Seq(
        (8, "Henry", "henry@example.com"),
        (9, "Ivy", "ivy@example.com")
    )
    val newDF = newData.toDF("id", "name", "email")
    
    // Append to existing table
    newDF.writeTo("users").append()
    
    // Alternative: use traditional write API with mode
    newDF.write.mode("append").saveAsTable("users")
    ```

=== "Java"
    ```java
    // Create new data
    List<Row> newData = Arrays.asList(
        RowFactory.create(8L, "Henry", "henry@example.com"),
        RowFactory.create(9L, "Ivy", "ivy@example.com")
    );
    Dataset<Row> newDF = spark.createDataFrame(newData, schema);

    // Append to existing table
    newDF.writeTo("users").append();

    // Alternative: use traditional write API with mode
    newDF.write().mode("append").saveAsTable("users");
    ```

## Writing Blob Data

When writing binary data to blob columns, you need to add metadata to the DataFrame schema to indicate blob encoding.

=== "Python"
    ```python
    from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BinaryType

    # Create schema with blob metadata
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("title", StringType(), True),
        StructField("content", BinaryType(), True,
                   metadata={"lance-encoding:blob": "true"})
    ])

    # Create data with large binary content
    data = [
        (1, "Document 1", bytearray(b"Large binary content..." * 10000)),
        (2, "Document 2", bytearray(b"Another large file..." * 10000))
    ]

    df = spark.createDataFrame(data, schema)

    # Write to blob table
    df.writeTo("documents").append()
    ```

=== "Scala"
    ```scala
    import org.apache.spark.sql.types._

    // Create schema with blob metadata
    val metadata = new MetadataBuilder()
      .putString("lance-encoding:blob", "true")
      .build()

    val schema = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("title", StringType, nullable = true),
      StructField("content", BinaryType, nullable = true, metadata)
    ))

    // Create data with large binary content
    val data = Seq(
      (1, "Document 1", Array.fill[Byte](1000000)(0x42)),
      (2, "Document 2", Array.fill[Byte](1000000)(0x43))
    )

    val df = spark.createDataFrame(data).toDF("id", "title", "content")

    // Write to blob table
    df.writeTo("documents").append()
    ```

=== "Java"
    ```java
    import org.apache.spark.sql.types.*;
    import java.util.Arrays;
    import java.util.List;

    // Create schema with blob metadata
    Metadata blobMetadata = new MetadataBuilder()
        .putString("lance-encoding:blob", "true")
        .build();

    StructType schema = new StructType(new StructField[]{
        DataTypes.createStructField("id", DataTypes.IntegerType, false),
        DataTypes.createStructField("title", DataTypes.StringType, true),
        DataTypes.createStructField("content", DataTypes.BinaryType, true, blobMetadata)
    });

    // Create data with large binary content
    byte[] largeData1 = new byte[1000000];
    byte[] largeData2 = new byte[1000000];
    Arrays.fill(largeData1, (byte) 0x42);
    Arrays.fill(largeData2, (byte) 0x43);

    List<Row> data = Arrays.asList(
        RowFactory.create(1, "Document 1", largeData1),
        RowFactory.create(2, "Document 2", largeData2)
    );

    Dataset<Row> df = spark.createDataFrame(data, schema);

    // Write to blob table
    df.writeTo("documents").append();
    ```

**Important Notes**:

- Blob metadata must be added to the DataFrame schema using the key `"lance-encoding:blob"` with value `"true"`
- The binary column must be nullable in the schema
- Blob encoding is most beneficial for large binary values (typically > 64KB)
- When writing to an existing blob table, ensure the schema metadata matches the table definition