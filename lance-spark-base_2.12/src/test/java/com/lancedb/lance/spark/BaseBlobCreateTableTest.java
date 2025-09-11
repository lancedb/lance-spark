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

import com.lancedb.lance.namespace.dir.DirectoryNamespaceConfig;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public abstract class BaseBlobCreateTableTest {
  private SparkSession spark;
  private static final String catalogName = "lance_ns";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() {
    spark =
        SparkSession.builder()
            .appName("blob-create-table-test")
            .master("local[*]")
            .config(
                "spark.sql.catalog." + catalogName,
                "com.lancedb.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", "dir")
            .config(
                "spark.sql.catalog." + catalogName + "." + DirectoryNamespaceConfig.ROOT,
                tempDir.toString())
            .getOrCreate();
  }

  @AfterEach
  void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testCreateTableWithBlobColumn() {
    String tableName = "blob_table_" + System.currentTimeMillis();

    // Create table with blob column using TBLPROPERTIES
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "data BINARY"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'data.lance.encoding' = 'blob'"
            + ")");

    // Verify table was created
    Dataset<Row> tables = spark.sql("SHOW TABLES IN " + catalogName + ".default");
    List<Row> tableList = tables.collectAsList();
    boolean found = tableList.stream().anyMatch(row -> tableName.equals(row.getString(1)));
    assertTrue(found, "Table should be created");

    // Insert data into the table
    List<Row> rows = new ArrayList<>();
    Random random = new Random(42);
    for (int i = 0; i < 10; i++) {
      // Create large binary data (> 64KB to ensure blob encoding is needed)
      byte[] largeData = new byte[100000]; // 100KB
      random.nextBytes(largeData);
      rows.add(RowFactory.create(i, largeData));
    }

    // Create DataFrame with proper schema
    Metadata blobMetadata = new MetadataBuilder().putString("lance-encoding:blob", "true").build();
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("data", DataTypes.BinaryType, true, blobMetadata)
            });

    Dataset<Row> df = spark.createDataFrame(rows, schema);
    try {
      df.writeTo(catalogName + ".default." + tableName).append();
    } catch (Exception e) {
      fail("Failed to append data to table: " + e.getMessage());
    }

    // Query the table
    Dataset<Row> result =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(10L, result.collectAsList().get(0).getLong(0));

    // Verify we can read the blob data back
    Dataset<Row> dataResult =
        spark.sql(
            "SELECT id, data FROM " + catalogName + ".default." + tableName + " WHERE id = 0");
    List<Row> dataRows = dataResult.collectAsList();
    assertEquals(1, dataRows.size());
    assertEquals(0, dataRows.get(0).getInt(0));
    byte[] retrievedData = (byte[]) dataRows.get(0).get(1);
    assertEquals(100000, retrievedData.length);

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testCreateEmptyTableWithBlobAndSQLInsert() {
    String tableName = "blob_empty_table_" + System.currentTimeMillis();

    // Create empty table with blob column using TBLPROPERTIES
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "text STRING, "
            + "blob_data BINARY"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'blob_data.lance.encoding' = 'blob'"
            + ")");

    // Verify table was created
    Dataset<Row> tables = spark.sql("SHOW TABLES IN " + catalogName + ".default");
    List<Row> tableList = tables.collectAsList();
    boolean found = tableList.stream().anyMatch(row -> tableName.equals(row.getString(1)));
    assertTrue(found, "Table should be created");

    // Insert data using SQL (with smaller test data for SQL insert)
    String testData1 = "This is test blob data 1";
    String testData2 = "This is test blob data 2";
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES "
            + "(1, 'first text', X'"
            + bytesToHex(testData1.getBytes(StandardCharsets.UTF_8))
            + "'), "
            + "(2, 'second text', X'"
            + bytesToHex(testData2.getBytes(StandardCharsets.UTF_8))
            + "')");

    // Query the table to verify data was inserted
    Dataset<Row> result =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertEquals(2L, result.collectAsList().get(0).getLong(0));

    // Query with projection
    Dataset<Row> projection =
        spark.sql("SELECT id, text FROM " + catalogName + ".default." + tableName + " ORDER BY id");
    List<Row> rows = projection.collectAsList();
    assertEquals(2, rows.size());
    assertEquals(1, rows.get(0).getInt(0));
    assertEquals("first text", rows.get(0).getString(1));
    assertEquals(2, rows.get(1).getInt(0));
    assertEquals("second text", rows.get(1).getString(1));

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testCreateTableWithMultipleBlobColumns() {
    String tableName = "blob_table_multi_" + System.currentTimeMillis();

    // Create table with multiple blob columns using TBLPROPERTIES
    spark.sql(
        "CREATE TABLE IF NOT EXISTS "
            + catalogName
            + ".default."
            + tableName
            + " ("
            + "id INT NOT NULL, "
            + "blob1 BINARY, "
            + "regular_binary BINARY, "
            + "blob2 BINARY"
            + ") USING lance "
            + "TBLPROPERTIES ("
            + "'blob1.lance.encoding' = 'blob', "
            + "'blob2.lance.encoding' = 'blob'"
            + ")");

    // Verify table was created
    Dataset<Row> tables = spark.sql("SHOW TABLES IN " + catalogName + ".default");
    List<Row> tableList = tables.collectAsList();
    boolean found = tableList.stream().anyMatch(row -> tableName.equals(row.getString(1)));
    assertTrue(found, "Table should be created");

    // Clean up
    spark.sql("DROP TABLE IF EXISTS " + catalogName + ".default." + tableName);
  }

  @Test
  public void testCreateTableWithInvalidBlobType() {
    String tableName = "blob_table_invalid_" + System.currentTimeMillis();

    // Try to create table with non-binary blob column (should fail)
    try {
      spark.sql(
          "CREATE TABLE IF NOT EXISTS "
              + catalogName
              + ".default."
              + tableName
              + " ("
              + "id INT NOT NULL, "
              + "blob_data STRING"
              + ") USING lance "
              + "TBLPROPERTIES ("
              + "'blob_data.lance.encoding' = 'blob'"
              + ")");
      fail("Should throw exception for non-binary blob column");
    } catch (Exception e) {
      // Expected exception
      assertTrue(
          e.getMessage().contains("must have BINARY type")
              || e.getCause().getMessage().contains("must have BINARY type"));
    }
  }

  private String bytesToHex(byte[] bytes) {
    StringBuilder hexString = new StringBuilder();
    for (byte b : bytes) {
      hexString.append(String.format("%02X", b));
    }
    return hexString.toString();
  }
}
