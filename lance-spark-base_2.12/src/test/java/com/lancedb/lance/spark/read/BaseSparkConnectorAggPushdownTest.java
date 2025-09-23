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
package com.lancedb.lance.spark.read;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public abstract class BaseSparkConnectorAggPushdownTest {
  private static SparkSession spark;

  @TempDir static Path tempDir;

  @BeforeAll
  static void setup() {
    spark =
        SparkSession.builder()
            .appName("LanceAggregatePushDownTest")
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalog.lance", "com.lancedb.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog.lance.impl", "dir")
            .config("spark.sql.catalog.lance.root", tempDir.toString())
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  public void testCountStarPushDown() throws Exception {
    String tableName = "lance.default.count_test_dataset";
    spark.range(0, 100).toDF("id").repartition(4).writeTo(tableName).create();

    Dataset<Row> lanceDataset = spark.table(tableName);
    lanceDataset.selectExpr("count(*)").explain(true);
    Dataset<Row> countDataset = lanceDataset.selectExpr("count(*)");
    Row countRow = countDataset.first();
    long countFromSelectExpr = countRow.getLong(0);
    long count = lanceDataset.count();
    assertEquals(100L, countFromSelectExpr, "Count(*) should return 100");
    assertEquals(100L, count, "Count should return 100 rows");
  }

  @Test
  public void testCountStarWithFilter() throws Exception {
    String tableName = "lance.default.count_filter_test_dataset";

    // Create test data using catalog table
    spark
        .range(0, 100)
        .selectExpr("id", "id % 10 as category", "id * 2 as value")
        .repartition(4)
        .writeTo(tableName)
        .create();

    Dataset<Row> lanceDataset = spark.table(tableName);

    long filteredCount = lanceDataset.filter("category = 5").count();
    lanceDataset.explain(true);
    assertEquals(10, filteredCount, "Filtered count should return 10 rows");

    long complexFilteredCount = lanceDataset.filter("category > 5 AND value < 150").count();
    // category > 5 means 6,7,8,9 (4 categories)
    // value < 150 means id < 75 (since value = id * 2)
    // Each category has 7 values < 75, so 4 * 7 = 28
    assertEquals(28, complexFilteredCount, "Complex filtered count should return 28 rows");
  }

  @Test
  public void testMultipleAggregates() throws Exception {
    String tableName = "lance.default.multiple_agg_test_dataset";

    // Create test data using catalog table
    spark
        .range(1, 101)
        .selectExpr("id", "id * 10 as value")
        .repartition(4)
        .writeTo(tableName)
        .create();

    Dataset<Row> lanceDataset = spark.table(tableName);

    Dataset<Row> aggregates =
        lanceDataset.selectExpr("count(*) as cnt", "sum(value) as total", "avg(value) as average");

    Row result = aggregates.first();
    assertEquals(100L, result.getLong(0), "Count should be 100");
    assertEquals(50500L, result.getLong(1), "Sum should be 50500");
    assertEquals(505.0, result.getDouble(2), 0.001, "Average should be 505");
  }

  @Test
  public void testCountColumnNotPushedDown() throws Exception {
    String tableName = "lance.default.count_column_test_dataset";

    // Create test data with some nulls
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1L, "a"),
                RowFactory.create(2L, null),
                RowFactory.create(3L, "c"),
                RowFactory.create(4L, null),
                RowFactory.create(5L, "e")),
            new StructType()
                .add("id", org.apache.spark.sql.types.DataTypes.LongType)
                .add("name", org.apache.spark.sql.types.DataTypes.StringType))
        .writeTo(tableName)
        .create();

    // Force a refresh of the catalog
    spark.catalog().refreshTable(tableName);

    Dataset<Row> lanceDataset = spark.table(tableName);

    // COUNT(column) should not be pushed down (it excludes nulls)
    long countName = lanceDataset.selectExpr("count(name)").first().getLong(0);
    assertEquals(3L, countName, "Count(name) should be 3 (excluding nulls)");

    // COUNT(*) should still be pushed down
    long countStar = lanceDataset.selectExpr("count(*)").first().getLong(0);
    assertEquals(5L, countStar, "Count(*) should be 5");
  }

  @Test
  public void testCountDistinctNotPushedDown() throws Exception {
    String tableName = "lance.default.count_distinct_test_dataset";

    // Create test data with duplicates
    spark
        .createDataFrame(
            Arrays.asList(
                RowFactory.create(1L, "a"),
                RowFactory.create(2L, "b"),
                RowFactory.create(3L, "a"),
                RowFactory.create(4L, "b"),
                RowFactory.create(5L, "c")),
            new StructType()
                .add("id", org.apache.spark.sql.types.DataTypes.LongType)
                .add("category", org.apache.spark.sql.types.DataTypes.StringType))
        .writeTo(tableName)
        .create();

    // Force a refresh of the catalog
    spark.catalog().refreshTable(tableName);

    Dataset<Row> lanceDataset = spark.table(tableName);

    // COUNT(DISTINCT column) should not be pushed down
    long countDistinct = lanceDataset.selectExpr("count(distinct category)").first().getLong(0);
    assertEquals(3L, countDistinct, "Count(distinct category) should be 3");
  }
}
