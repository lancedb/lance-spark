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
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class LanceNamespaceTestBase {
  protected SparkSession spark;
  protected TableCatalog catalog;
  protected String catalogName = "lance_ns";

  @TempDir protected Path tempDir;

  @BeforeEach
  void setup() throws IOException {
    spark =
        SparkSession.builder()
            .appName("lance-namespace-test")
            .master("local")
            .config(
                "spark.sql.catalog." + catalogName,
                "com.lancedb.lance.spark.LanceNamespaceSparkCatalog")
            .config("spark.sql.catalog." + catalogName + ".impl", getNsImpl())
            .getOrCreate();

    Map<String, String> additionalConfigs = getAdditionalNsConfigs();
    for (Map.Entry<String, String> entry : additionalConfigs.entrySet()) {
      spark.conf().set("spark.sql.catalog." + catalogName + "." + entry.getKey(), entry.getValue());
    }

    catalog = (TableCatalog) spark.sessionState().catalogManager().catalog(catalogName);
  }

  @AfterEach
  void tearDown() throws IOException {
    if (spark != null) {
      spark.stop();
    }
  }

  protected abstract String getNsImpl();

  protected Map<String, String> getAdditionalNsConfigs() {
    return new HashMap<>();
  }

  /**
   * Generates a unique table name with UUID suffix to avoid conflicts.
   *
   * @param baseName the base name for the table
   * @return unique table name with UUID suffix
   */
  protected String generateTableName(String baseName) {
    return baseName + "_" + UUID.randomUUID().toString().replace("-", "");
  }

  @Test
  public void testCreateAndDescribeTable() throws Exception {
    String tableName = generateTableName("test_table");

    // Create table using Spark SQL DDL
    spark.sql(
        "CREATE TABLE "
            + catalogName
            + ".default."
            + tableName
            + " (id BIGINT NOT NULL, name STRING)");

    // Describe table using Spark SQL
    Dataset<Row> describeResult =
        spark.sql("DESCRIBE TABLE " + catalogName + ".default." + tableName);
    List<Row> columns = describeResult.collectAsList();

    // Verify table structure
    assertEquals(2, columns.size());

    // Check id column
    Row idColumn = columns.get(0);
    assertEquals("id", idColumn.getString(0));
    assertEquals("bigint", idColumn.getString(1));

    // Check name column
    Row nameColumn = columns.get(1);
    assertEquals("name", nameColumn.getString(0));
    assertEquals("string", nameColumn.getString(1));
  }

  @Test
  public void testListTables() throws Exception {
    String tableName1 = generateTableName("list_test_1");
    String tableName2 = generateTableName("list_test_2");

    // Create tables using Spark SQL
    spark.sql("CREATE TABLE " + catalogName + ".default." + tableName1 + " (id BIGINT NOT NULL)");
    spark.sql("CREATE TABLE " + catalogName + ".default." + tableName2 + " (id BIGINT NOT NULL)");

    // Use SHOW TABLES to list tables
    Dataset<Row> tablesResult = spark.sql("SHOW TABLES IN " + catalogName + ".default");
    List<Row> tables = tablesResult.collectAsList();

    assertTrue(tables.size() >= 2);

    boolean foundTable1 = false;
    boolean foundTable2 = false;
    for (Row row : tables) {
      String tableName = row.getString(1); // table name is in the second column
      if (tableName1.equals(tableName)) {
        foundTable1 = true;
      }
      if (tableName2.equals(tableName)) {
        foundTable2 = true;
      }
    }
    assertTrue(foundTable1);
    assertTrue(foundTable2);
  }

  @Test
  public void testDropTable() throws Exception {
    String tableName = generateTableName("drop_test");

    // Create table using Spark SQL
    spark.sql("CREATE TABLE " + catalogName + ".default." + tableName + " (id BIGINT NOT NULL)");

    // Verify table exists by querying it
    Dataset<Row> result =
        spark.sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName);
    assertNotNull(result);
    assertEquals(0L, result.collectAsList().get(0).getLong(0));

    // Drop table using Spark SQL
    spark.sql("DROP TABLE " + catalogName + ".default." + tableName);

    // Verify table no longer exists
    assertThrows(
        Exception.class,
        () -> {
          spark
              .sql("SELECT COUNT(*) FROM " + catalogName + ".default." + tableName)
              .collectAsList();
        });
  }

  @Test
  public void testLoadSparkTable() throws Exception {
    // Test successful case - create table and load it
    String existingTableName = generateTableName("existing_table");

    // Create table using Spark SQL
    spark.sql(
        "CREATE TABLE "
            + catalogName
            + ".default."
            + existingTableName
            + " (id BIGINT NOT NULL, name STRING)");

    // Insert test data
    spark.sql(
        "INSERT INTO " + catalogName + ".default." + existingTableName + " VALUES (1, 'test')");

    // Successfully load existing table using spark.table()
    Dataset<Row> table = spark.table(catalogName + ".default." + existingTableName);
    assertNotNull(table);
    assertEquals(1, table.count());

    // Test failure case - try to load non-existent table
    String nonExistentTableName = generateTableName("non_existent");

    // Verify loading non-existent table throws exception
    assertThrows(
        Exception.class,
        () -> {
          spark.table(catalogName + ".default." + nonExistentTableName);
        });
  }

  @Test
  public void testSparkSqlSelect() throws Exception {
    String tableName = generateTableName("sql_test_table");

    // Create a table using SQL DDL
    spark.sql(
        "CREATE TABLE "
            + catalogName
            + ".default."
            + tableName
            + " (id INT NOT NULL, name STRING, value DOUBLE)");

    // Create test data and insert using SQL
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES "
            + "(1, 'Alice', 100.0), "
            + "(2, 'Bob', 200.0), "
            + "(3, 'Charlie', 300.0)");

    // Query using Spark SQL with catalog notation
    Dataset<Row> result = spark.sql("SELECT * FROM " + catalogName + ".default." + tableName);
    assertEquals(3, result.count());

    // Test filtering
    Dataset<Row> filtered =
        spark.sql("SELECT * FROM " + catalogName + ".default." + tableName + " WHERE id > 1");
    assertEquals(2, filtered.count());

    // Test aggregation
    Dataset<Row> aggregated =
        spark.sql("SELECT COUNT(*) as cnt FROM " + catalogName + ".default." + tableName);
    assertEquals(3L, aggregated.collectAsList().get(0).getLong(0));

    // Test projection
    Dataset<Row> projected =
        spark.sql(
            "SELECT name, value FROM " + catalogName + ".default." + tableName + " WHERE id = 2");
    Row row = projected.collectAsList().get(0);
    assertEquals("Bob", row.getString(0));
    assertEquals(200.0, row.getDouble(1), 0.001);
  }

  @Test
  public void testSparkSqlJoin() throws Exception {
    String tableName1 = generateTableName("join_table_1");
    String tableName2 = generateTableName("join_table_2");

    // Create first table using SQL DDL
    spark.sql(
        "CREATE TABLE "
            + catalogName
            + ".default."
            + tableName1
            + " (id INT NOT NULL, name STRING)");

    // Insert data into first table
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName1
            + " VALUES "
            + "(1, 'Alice'), "
            + "(2, 'Bob'), "
            + "(3, 'Charlie')");

    // Create second table using SQL DDL
    spark.sql(
        "CREATE TABLE " + catalogName + ".default." + tableName2 + " (id INT NOT NULL, score INT)");

    // Insert data into second table
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName2
            + " VALUES "
            + "(1, 95), "
            + "(2, 87), "
            + "(3, 92)");

    // Test join query
    Dataset<Row> joined =
        spark.sql(
            "SELECT t1.name, t2.score FROM "
                + catalogName
                + ".default."
                + tableName1
                + " t1 "
                + "JOIN "
                + catalogName
                + ".default."
                + tableName2
                + " t2 ON t1.id = t2.id");
    assertEquals(3, joined.count());

    // Verify join results
    List<Row> results = joined.orderBy("name").collectAsList();
    assertEquals("Alice", results.get(0).getString(0));
    assertEquals(95, results.get(0).getInt(1));
    assertEquals("Bob", results.get(1).getString(0));
    assertEquals(87, results.get(1).getInt(1));
    assertEquals("Charlie", results.get(2).getString(0));
    assertEquals(92, results.get(2).getInt(1));
  }
}
