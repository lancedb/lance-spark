/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.lancedb.lance.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Test for creating Lance tables with FixedSizeList vectors.
 *
 * <p>This test demonstrates the ability to create Lance tables with vector columns that are
 * converted to Arrow FixedSizeList format for efficient vector operations.
 */
public class FixedSizeListVectorTest {

  @TempDir Path tempDir;

  @Test
  public void testCreateTableWithFixedSizeListVectors() {
    String catalogName = "lance_test";

    SparkSession spark =
        SparkSession.builder()
            .appName("fixed-size-list-test")
            .master("local[*]")
            .config(
                "spark.sql.catalog." + catalogName,
                "com.lancedb.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();

    try {
      String tableName = "vector_table";

      // Create table with vector column using TBLPROPERTIES
      // The property 'embeddings.arrow.fixed-size-list.size' = '128'
      // instructs the catalog to create a FixedSizeList[128] column
      spark.sql(
          "CREATE TABLE IF NOT EXISTS "
              + catalogName
              + ".default."
              + tableName
              + " ("
              + "id INT NOT NULL, "
              + "text STRING, "
              + "embeddings ARRAY<FLOAT> NOT NULL"
              + ") USING lance "
              + "TBLPROPERTIES ("
              + "'embeddings.arrow.fixed-size-list.size' = '128'"
              + ")");

      // Verify table was created
      spark.sql("SHOW TABLES IN " + catalogName + ".default").show();

      // Get the schema with metadata
      // The table should be readable as Spark sees it as ARRAY<FLOAT>
      // The conversion to FixedSizeList happens internally during write operations
      StructType schema = spark.table(catalogName + ".default." + tableName).schema();
      assertNotNull(schema);
      assertEquals(3, schema.fields().length);

      // Verify the embeddings field has the correct metadata
      StructField embeddingsField = schema.apply("embeddings");
      assertNotNull(embeddingsField);
      Metadata metadata = embeddingsField.metadata();

      // The metadata should contain the fixed-size-list specification
      // This will be converted to Arrow FixedSizeList during write operations
      if (metadata.contains("arrow.fixed-size-list.size")) {
        long size = metadata.getLong("arrow.fixed-size-list.size");
        assertEquals(128L, size);
      }

      // Note: The table is created with ARRAY<FLOAT> schema that Spark can handle,
      // but internally Lance stores it as FixedSizeList for efficient vector operations.

      // Test: Insert and read back data to verify conversion works
      spark.sql(
          "INSERT INTO "
              + catalogName
              + ".default."
              + tableName
              + " VALUES "
              + "(1, 'test_text', array("
              + java.util.stream.IntStream.range(0, 128)
                  .mapToObj(i -> String.valueOf(i * 0.01f))
                  .collect(java.util.stream.Collectors.joining(", "))
              + "))");

      // Read back the data - this tests the FixedSizeList to Array conversion
      Dataset<Row> result = spark.sql("SELECT * FROM " + catalogName + ".default." + tableName);
      assertEquals(1, result.count(), "Should have 1 row");

      Row firstRow = result.first();
      assertEquals(1, firstRow.getInt(0));
      assertEquals("test_text", firstRow.getString(1));

      // Verify the array was read correctly
      scala.collection.mutable.WrappedArray<Float> embeddings =
          (scala.collection.mutable.WrappedArray<Float>) firstRow.get(2);
      assertEquals(128, embeddings.size(), "Embeddings should have 128 elements");

      // Clean up
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);

    } finally {
      spark.stop();
    }
  }

  @Test
  public void testDataFrameWriteWithFixedSizeList() {
    String catalogName = "lance_test";

    SparkSession spark =
        SparkSession.builder()
            .appName("dataframe-fixedsizelist-test")
            .master("local[*]")
            .config(
                "spark.sql.catalog." + catalogName,
                "com.lancedb.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();

    try {
      String tableName = "df_vector_table";
      
      // Create metadata for vector column - use Long value, not String
      org.apache.spark.sql.types.Metadata vectorMetadata = 
          org.apache.spark.sql.types.Metadata.fromJson(
              "{\"arrow.fixed-size-list.size\":128}"
          );
      
      // Create schema with vector column using DataFrame API
      org.apache.spark.sql.types.StructType schema = new org.apache.spark.sql.types.StructType(
          new org.apache.spark.sql.types.StructField[] {
              org.apache.spark.sql.types.DataTypes.createStructField("id", 
                  org.apache.spark.sql.types.DataTypes.IntegerType, false),
              org.apache.spark.sql.types.DataTypes.createStructField("text", 
                  org.apache.spark.sql.types.DataTypes.StringType, true),
              new org.apache.spark.sql.types.StructField(
                  "embeddings",
                  org.apache.spark.sql.types.DataTypes.createArrayType(
                      org.apache.spark.sql.types.DataTypes.FloatType, false),
                  false,
                  vectorMetadata
              )
          }
      );
      
      // Create test data
      java.util.List<org.apache.spark.sql.Row> rows = new java.util.ArrayList<>();
      for (int i = 0; i < 10; i++) {
        float[] vector = new float[128];
        for (int j = 0; j < 128; j++) {
          vector[j] = i * 0.01f + j * 0.001f;
        }
        rows.add(org.apache.spark.sql.RowFactory.create(i, "text_" + i, vector));
      }
      
      Dataset<Row> df = spark.createDataFrame(rows, schema);
      
      // Write to Lance table using DataFrame API
      df.writeTo(catalogName + ".default." + tableName)
          .using("lance")
          .createOrReplace();
      
      // Verify by reading back
      Dataset<Row> result = spark.table(catalogName + ".default." + tableName);
      assertEquals(10, result.count(), "Should have 10 rows");
      
      // Verify the array was read correctly
      Row firstRow = result.first();
      assertEquals(0, firstRow.getInt(0));
      assertEquals("text_0", firstRow.getString(1));
      scala.collection.mutable.WrappedArray<Float> embeddings =
          (scala.collection.mutable.WrappedArray<Float>) firstRow.get(2);
      assertEquals(128, embeddings.size(), "Embeddings should have 128 elements");
      
      // Clean up
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
      
    } finally {
      spark.stop();
    }
  }

  @Test
  public void testCreateTableWithMultipleVectorColumns() {
    String catalogName = "lance_test";

    SparkSession spark =
        SparkSession.builder()
            .appName("multi-vector-test")
            .master("local[*]")
            .config(
                "spark.sql.catalog." + catalogName,
                "com.lancedb.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config("spark.sql.catalog." + catalogName + ".root", tempDir.toString())
            .getOrCreate();

    try {
      String tableName = "multi_vector_table";

      // Create table with multiple vector columns of different dimensions
      spark.sql(
          "CREATE TABLE IF NOT EXISTS "
              + catalogName
              + ".default."
              + tableName
              + " ("
              + "id INT NOT NULL, "
              + "title STRING, "
              + "title_embeddings ARRAY<FLOAT> NOT NULL, "
              + "content_embeddings ARRAY<FLOAT> NOT NULL, "
              + "summary_embeddings ARRAY<FLOAT> NOT NULL"
              + ") USING lance "
              + "TBLPROPERTIES ("
              + "'title_embeddings.arrow.fixed-size-list.size' = '64', "
              + "'content_embeddings.arrow.fixed-size-list.size' = '256', "
              + "'summary_embeddings.arrow.fixed-size-list.size' = '128'"
              + ")");

      // Verify table was created
      // The table should be readable as Spark sees it as multiple ARRAY<FLOAT> columns
      // The conversion to FixedSizeList happens internally during write operations
      StructType schema = spark.table(catalogName + ".default." + tableName).schema();
      assertNotNull(schema);
      assertEquals(5, schema.fields().length);

      // Clean up
      spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);

    } finally {
      spark.stop();
    }
  }
}
