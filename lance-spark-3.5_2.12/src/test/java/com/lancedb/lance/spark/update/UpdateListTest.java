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

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

public class UpdateListTest extends UpdateBase {

  @Test
  public void testUpdateSomeRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();
    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", Arrays.asList(100)),
            Row.of(2, "Bob", Arrays.asList(200)),
            Row.of(3, "Charlie", Arrays.asList(300))));

    op.update(Arrays.asList(100, 200), "id = 1 or id = 2");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", Arrays.asList(100, 200)),
            Row.of(2, "Bob", Arrays.asList(100, 200)),
            Row.of(3, "Charlie", Arrays.asList(300))));
  }

  @Test
  public void testUpdateAllRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();
    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", Arrays.asList(100)),
            Row.of(2, "Bob", Arrays.asList(200)),
            Row.of(3, "Charlie", Arrays.asList(300))));

    op.update(Arrays.asList(100, 200), "id > 0");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", Arrays.asList(100, 200)),
            Row.of(2, "Bob", Arrays.asList(100, 200)),
            Row.of(3, "Charlie", Arrays.asList(100, 200))));
  }

  @Test
  public void testTransformSomeRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();
    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", Arrays.asList(100)),
            Row.of(2, "Bob", Arrays.asList(200)),
            Row.of(3, "Charlie", Arrays.asList(300))));

    op.update("transform(values, x -> x + 1)", "id = 1 or id = 2");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", Arrays.asList(101)),
            Row.of(2, "Bob", Arrays.asList(201)),
            Row.of(3, "Charlie", Arrays.asList(300))));
  }

  @Test
  public void testTransformAllRows() {
    TableOperator op = new TableOperator(spark, catalogName);
    op.create();
    op.insert(
        Arrays.asList(
            Row.of(1, "Alice", Arrays.asList(100, 102)),
            Row.of(2, "Bob", Arrays.asList(200)),
            Row.of(3, "Charlie", Arrays.asList(300))));

    op.update("transform(values, x -> x + 1)", "id > 0");
    op.check(
        Arrays.asList(
            Row.of(1, "Alice", Arrays.asList(101, 103)),
            Row.of(2, "Bob", Arrays.asList(201)),
            Row.of(3, "Charlie", Arrays.asList(301))));
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
              + " (id INT NOT NULL, name STRING NOT NULL, values ARRAY<INT>)");
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

    public void update(List<Integer> values, String condition) {
      String sql =
          String.format(
              "Update %s.default.%s set values=ARRAY(%s) where %s",
              catalogName,
              tableName,
              values.stream().map(String::valueOf).collect(Collectors.joining(",")),
              condition);
      spark.sql(sql);
    }

    public void update(String transform, String condition) {
      String sql =
          String.format(
              "Update %s.default.%s set values=%s where %s",
              catalogName, tableName, transform, condition);
      spark.sql(sql);
    }

    public void check(List<Row> expected) {
      String sql = String.format("Select * from %s.default.%s order by id", catalogName, tableName);
      List<Row> actual =
          spark.sql(sql).collectAsList().stream()
              .map(row -> Row.of(row.getInt(0), row.getString(1), row.getList(2)))
              .collect(Collectors.toList());
      Assertions.assertEquals(expected, actual);
    }
  }

  private static class Row {
    int id;
    String name;
    List<Integer> values;

    private static Row of(int id, String name, List<Integer> value) {
      Row row = new Row();
      row.id = id;
      row.name = name;
      row.values = value;
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
      return id == row.id
          && Objects.equals(name, row.name)
          && Objects.deepEquals(values, row.values);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, values);
    }

    @Override
    public String toString() {
      return String.format("Row(id=%s, name=%s, values=%s)", id, name, values);
    }

    private String insertSql() {
      return String.format(
          "(%d, '%s', ARRAY(%s))",
          id, name, values.stream().map(String::valueOf).collect(Collectors.joining(",")));
    }
  }
}
