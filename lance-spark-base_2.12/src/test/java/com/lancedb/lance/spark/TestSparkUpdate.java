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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

/** Test for LanceNamespaceSparkCatalog using DirectoryNamespace implementation. */
public class TestSparkUpdate extends SparkLanceNamespaceTestBase {

  @Override
  protected String getNsImpl() {
    return "dir";
  }

  @Override
  protected Map<String, String> getAdditionalNsConfigs() {
    Map<String, String> configs = new HashMap<>();
    configs.put(DirectoryNamespaceConfig.ROOT, tempDir.toString());
    return configs;
  }

  @Test
  public void testSparkSqlUpdate() throws Exception {
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

    String select = "Select * from " + catalogName + ".default." + tableName;
    System.out.println(spark.sql(select).collectAsList());

    spark.sql(
        "Update " + catalogName + ".default." + tableName + " set value = value + 1 where id = 2");

    System.out.println("===============");
    System.out.println(spark.sql(select).collectAsList());
  }
}
