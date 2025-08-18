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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class UpdateTableTest {
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

  protected String getNsImpl() {
    return "dir";
  }

  protected Map<String, String> getAdditionalNsConfigs() {
    Map<String, String> configs = new HashMap<>();
    configs.put(DirectoryNamespaceConfig.ROOT, tempDir.toString());
    return configs;
  }

  @Test
  public void testSparkSqlUpdate() throws Exception {
    String baseName = "sql_test_table";
    String tableName = baseName + "_" + UUID.randomUUID().toString().replace("-", "");

    // Create a table using SQL DDL
    spark.sql(
        "CREATE TABLE "
            + catalogName
            + ".default."
            + tableName
            + " (id INT NOT NULL, name STRING, value INT)");

    // Create test data and insert using SQL
    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES "
            + "(1, 'Alice', 100), "
            + "(2, 'Bob', 200), "
            + "(3, 'Charlie', 300)");

    spark.sql(
        "INSERT INTO "
            + catalogName
            + ".default."
            + tableName
            + " VALUES "
            + "(4, 'Tom', 100), "
            + "(5, 'Frank', 200)");

    spark.sql(
        "INSERT INTO " + catalogName + ".default." + tableName + " VALUES " + "(6, 'Penny', 200)");

    String select = "Select * from " + catalogName + ".default." + tableName + " order by id";

    spark.sql(
        "Update "
            + catalogName
            + ".default."
            + tableName
            + " set value = value + 1 where value = 200");

    Assertions.assertEquals(
        "[[1,Alice,100], [2,Bob,201], [3,Charlie,300], [4,Tom,100], [5,Frank,201], [6,Penny,201]]",
        spark.sql(select).collectAsList().toString());
  }
}
