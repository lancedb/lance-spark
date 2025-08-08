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
package com.lancedb.lance.spark.write;

import com.lancedb.lance.spark.LanceConfig;
import com.lancedb.lance.spark.LanceDataSource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class VectorWriteTest {
  private static SparkSession spark;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("vector-write-test")
            .master("local[*]")
            .config("spark.sql.execution.arrow.maxRecordsPerBatch", "1024")
            .config("spark.sql.catalog.lance", "com.lancedb.lance.spark.LanceCatalog")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testVectorColumnFloat32(TestInfo testInfo) {
    int numRows = 256;
    int vectorDim = 128;

    // Create metadata for vector column
    Metadata vectorMetadata =
        new MetadataBuilder().putLong("arrow.fixed-size-list.size", vectorDim).build();

    // Create schema with vector column
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField(
                  "embeddings",
                  DataTypes.createArrayType(DataTypes.FloatType, false),
                  false,
                  vectorMetadata)
            });

    // Create test data
    List<Row> rows = new ArrayList<>();
    Random random = new Random(42);
    for (int i = 0; i < numRows; i++) {
      float[] vector = new float[vectorDim];
      for (int j = 0; j < vectorDim; j++) {
        vector[j] = random.nextFloat();
      }
      rows.add(RowFactory.create(i, vector));
    }

    Dataset<Row> df = spark.createDataFrame(rows, schema);

    // Write to Lance format
    String outputPath = "/tmp/vector_test_float32.lance";
    deleteDirectory(new File(outputPath));

    df.write()
        .format(LanceDataSource.name)
        .option(LanceConfig.CONFIG_DATASET_URI, outputPath)
        .save();

    // Verify the dataset can be read back
    Dataset<Row> readDf = spark.read()
        .format(LanceDataSource.name)
        .option(LanceConfig.CONFIG_DATASET_URI, outputPath)
        .load();
    
    assert readDf.count() == numRows : "Row count mismatch";
    System.out.println("Float32 vector dataset written and verified at: " + outputPath);
  }

  @Test
  public void testVectorColumnFloat64(TestInfo testInfo) {
    int numRows = 300;
    int vectorDim = 64;

    // Create metadata for vector column
    Metadata vectorMetadata =
        new MetadataBuilder().putLong("arrow.fixed-size-list.size", vectorDim).build();

    // Create schema with vector column
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField(
                  "embeddings",
                  DataTypes.createArrayType(DataTypes.DoubleType, false),
                  false,
                  vectorMetadata)
            });

    // Create test data
    List<Row> rows = new ArrayList<>();
    Random random = new Random(42);
    for (int i = 0; i < numRows; i++) {
      double[] vector = new double[vectorDim];
      for (int j = 0; j < vectorDim; j++) {
        vector[j] = random.nextDouble();
      }
      rows.add(RowFactory.create(i, vector));
    }

    Dataset<Row> df = spark.createDataFrame(rows, schema);

    // Write to Lance format
    String outputPath = "/tmp/vector_test_float64.lance";
    deleteDirectory(new File(outputPath));

    df.write()
        .format(LanceDataSource.name)
        .option(LanceConfig.CONFIG_DATASET_URI, outputPath)
        .save();

    System.out.println("Float64 vector dataset written to: " + outputPath);
  }

  @Test
  public void testMixedVectorColumns(TestInfo testInfo) {
    int numRows = 500;
    int vectorDim1 = 128;
    int vectorDim2 = 256;

    // Create metadata for vector columns
    Metadata vectorMetadata1 =
        new MetadataBuilder().putLong("arrow.fixed-size-list.size", vectorDim1).build();

    Metadata vectorMetadata2 =
        new MetadataBuilder().putLong("arrow.fixed-size-list.size", vectorDim2).build();

    // Create schema with multiple vector columns
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField(
                  "embeddings_float",
                  DataTypes.createArrayType(DataTypes.FloatType, false),
                  false,
                  vectorMetadata1),
              DataTypes.createStructField(
                  "embeddings_double",
                  DataTypes.createArrayType(DataTypes.DoubleType, false),
                  false,
                  vectorMetadata2),
              DataTypes.createStructField(
                  "regular_array", DataTypes.createArrayType(DataTypes.IntegerType, false), true)
            });

    // Create test data
    List<Row> rows = new ArrayList<>();
    Random random = new Random(42);
    for (int i = 0; i < numRows; i++) {
      float[] vector1 = new float[vectorDim1];
      for (int j = 0; j < vectorDim1; j++) {
        vector1[j] = random.nextFloat();
      }

      double[] vector2 = new double[vectorDim2];
      for (int j = 0; j < vectorDim2; j++) {
        vector2[j] = random.nextDouble();
      }

      int[] regularArray = new int[random.nextInt(10) + 1];
      for (int j = 0; j < regularArray.length; j++) {
        regularArray[j] = random.nextInt(100);
      }

      rows.add(RowFactory.create(i, vector1, vector2, regularArray));
    }

    Dataset<Row> df = spark.createDataFrame(rows, schema);

    // Write to Lance format
    String outputPath = "/tmp/vector_test_mixed.lance";
    deleteDirectory(new File(outputPath));

    df.write()
        .format(LanceDataSource.name)
        .option(LanceConfig.CONFIG_DATASET_URI, outputPath)
        .save();

    System.out.println("Mixed vector dataset written to: " + outputPath);
  }

  private void deleteDirectory(File dir) {
    if (dir.exists()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            file.delete();
          }
        }
      }
      dir.delete();
    }
  }
}
