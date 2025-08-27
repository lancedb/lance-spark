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

import com.lancedb.lance.namespace.hive3.LocalHive3Metastore;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public abstract class BaseTestSparkHive3Namespace extends SparkLanceNamespaceTestBase {

  private LocalHive3Metastore metastore;

  @BeforeEach
  void setup() throws IOException {
    // Start local Hive metastore using default configuration
    metastore = new LocalHive3Metastore();
    metastore.start();

    // Call parent setup
    super.setup();
  }

  @AfterEach
  void tearDown() throws IOException {
    // Call parent teardown first
    super.tearDown();

    // Stop Hive metastore
    if (metastore != null) {
      try {
        metastore.stop();
      } catch (Exception e) {
        throw new IOException("Failed to stop Hive metastore", e);
      }
    }
  }

  @Override
  protected String getNsImpl() {
    return "hive3";
  }

  @Override
  protected Map<String, String> getAdditionalNsConfigs() {
    Map<String, String> configs = new HashMap<>();

    // Configure parent prefix for Hive3's 3-level namespace - use default Hive catalog
    configs.put("parent", "hive");
    configs.put("parent_delimiter", ".");

    // Configure Hive metastore connection using hadoop prefix for SparkUtil
    if (metastore != null && metastore.hiveConf() != null) {
      String metastoreUri = metastore.hiveConf().getVar(HiveConf.ConfVars.METASTOREURIS);
      configs.put("hadoop.hive.metastore.uris", metastoreUri);
    }
    configs.put("client.pool-size", "3");

    // Configure storage location
    configs.put("root", tempDir.toString() + "/lance");

    return configs;
  }

  @Override
  protected boolean supportsNamespace() {
    return true;
  }
}
