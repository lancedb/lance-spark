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
package com.lancedb.lance.spark.update;

import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.spark.LanceConfig;
import com.lancedb.lance.spark.LanceDataset;
import com.lancedb.lance.spark.internal.LanceDatasetAdapter;

import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public class UpdateTableTest extends UpdateBase {
  @Test
  public void testUpdateNoRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(Row.of(1, "Alice", 100), Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));

    op.update("value > 400");
    op.check(
        Arrays.asList(Row.of(1, "Alice", 100), Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));
  }

  @Test
  public void testUpdateSomeRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(Row.of(1, "Alice", 100), Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));

    op.update("value >= 200");
    op.check(
        Arrays.asList(Row.of(1, "Alice", 100), Row.of(2, "Bob", 201), Row.of(3, "Charlie", 301)));
  }

  @Test
  public void testUpdateAllRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(Row.of(1, "Alice", 100), Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));

    op.update("value > 0");
    op.check(
        Arrays.asList(Row.of(1, "Alice", 101), Row.of(2, "Bob", 201), Row.of(3, "Charlie", 301)));
  }

  @Test
  public void testUpdateMultipleTimes() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(Row.of(1, "Alice", 100), Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));

    // Update one row
    op.update("value = 100");
    op.check(
        Arrays.asList(Row.of(1, "Alice", 101), Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));

    // Update the same row multiple times
    op.update("value = 101");
    op.check(
        Arrays.asList(Row.of(1, "Alice", 102), Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));

    // Update other row
    op.update("value = 200");
    op.check(
        Arrays.asList(Row.of(1, "Alice", 102), Row.of(2, "Bob", 201), Row.of(3, "Charlie", 300)));

    op.update("value >= 200");
    op.check(
        Arrays.asList(Row.of(1, "Alice", 102), Row.of(2, "Bob", 202), Row.of(3, "Charlie", 301)));
  }

  @Test
  public void testUpdateMultiFragments() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(Row.of(1, "Alice", 100), Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));

    op.insert(Arrays.asList(Row.of(4, "Tom", 100), Row.of(5, "Frank", 200)));

    op.insert(Arrays.asList(Row.of(6, "Penny", 200)));

    op.update("value = 200");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", 100),
            Row.of(2, "Bob", 201),
            Row.of(3, "Charlie", 300),
            Row.of(4, "Tom", 100),
            Row.of(5, "Frank", 201),
            Row.of(6, "Penny", 201)));
  }

  @Test
  public void testUpdateDeletedRows() throws Exception {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();

    op.insert(
        Arrays.asList(Row.of(1, "Alice", 100), Row.of(2, "Bob", 200), Row.of(3, "Charlie", 300)));

    LanceDataset dataset =
        (LanceDataset) catalog.loadTable(catalog.listTables(catalog.defaultNamespace())[0]);
    LanceConfig config = dataset.config();

    List<Integer> fragmentIds = LanceDatasetAdapter.getFragmentIds(config);
    Assertions.assertEquals(1, fragmentIds.size());

    // Delete first two rows
    FragmentMetadata updateFragment =
        LanceDatasetAdapter.deleteRows(config, fragmentIds.get(0), ImmutableList.of(0, 1));
    Assertions.assertNotNull(updateFragment);
    LanceDatasetAdapter.updateFragments(
        config, Collections.emptyList(), ImmutableList.of(updateFragment), Collections.emptyList());

    op.check(Arrays.asList(Row.of(3, "Charlie", 300)));

    // Update all rows
    op.update("value > 0");
    op.check(Arrays.asList(Row.of(3, "Charlie", 301)));
  }

  private static class TableOperator {
    private final SparkSession spark;
    private final String catalogName;
    private final String tableName;

    public TableOperator(SparkSession spark, String catalogName) {
      this.spark = spark;

      this.catalogName = catalogName;

      String baseName = "sql_test_table";
      this.tableName = baseName + "_" + UUID.randomUUID().toString().replace("-", "");
    }

    public void create() {
      // Create a table using SQL DDL
      spark.sql(
          "CREATE TABLE "
              + catalogName
              + ".default."
              + tableName
              + " (id INT NOT NULL, name STRING, value INT)");
    }

    public void insert(List<Row> rows) {
      String sql =
          String.format(
              "INSERT INTO %s.default.%s VALUES %s",
              catalogName,
              tableName,
              rows.stream().map(Row::insertSql).collect(Collectors.joining(", ")));
      spark.sql(sql);
    }

    public void update(String condition) {
      String sql =
          String.format(
              "Update %s.default.%s set value = value + 1 where %s",
              catalogName, tableName, condition);
      spark.sql(sql);
    }

    public void check(List<Row> expected) {
      String sql = String.format("Select * from %s.default.%s order by id", catalogName, tableName);
      List<Row> actual =
          spark.sql(sql).collectAsList().stream()
              .map(row -> Row.of(row.getInt(0), row.getString(1), row.getInt(2)))
              .collect(Collectors.toList());
      Assertions.assertEquals(expected, actual);
    }
  }

  private static class Row {
    int id;
    String name;
    int value;

    private static Row of(int id, String name, int value) {
      Row row = new Row();
      row.id = id;
      row.name = name;
      row.value = value;
      return row;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Row row = (Row) o;
      return id == row.id && value == row.value && Objects.equals(name, row.name);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, value);
    }

    @Override
    public String toString() {
      return String.format("Row(id=%s, name=%s, value=%s)", id, name, value);
    }

    private String insertSql() {
      return String.format("(%d, '%s', %d)", id, name, value);
    }
  }
}
