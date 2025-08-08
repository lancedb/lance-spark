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

import java.util.HashMap;
import java.util.Map;

/** Test for LanceNamespaceSparkCatalog using DirectoryNamespace implementation. */
public class TestSparkDirectoryNamespace extends SparkLanceNamespaceTestBase {

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
}
