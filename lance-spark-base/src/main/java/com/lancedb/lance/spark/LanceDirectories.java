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

import com.lancedb.lance.WriteParams;
import com.lancedb.lance.spark.internal.LanceDatasetAdapter;
import com.lancedb.lance.spark.utils.Optional;
import com.lancedb.lance.spark.utils.PropertyUtils;

import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.Some;

import java.util.Map;

/**
 * A Spark catalog wrapper over Lance directories. User can configure a directory to be accessed
 * through the default namespace, or configure a mapping of Spark namespace to directory path in
 * order to access multiple directories.
 */
public class LanceDirectories implements TableCatalog, SupportsNamespaces {

  public static final String CATALOG_PROPERTY_PATH = "path";
  public static final String CATALOG_PROPERTY_PATHS_PREFIX = "paths.";
  public static final String DEFAULT_NAMESPACE = "default";
  public static final String NS_PROPERTY_PATH = "path";

  private String name;
  private Map<String, String> options;
  private Map<String, String> nsToPath;

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    throw new UnsupportedOperationException("createNamespace is not supported");
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("alterNamespace is not supported");
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    throw new UnsupportedOperationException("dropNamespace is not supported");
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return nsToPath.keySet().toArray(new String[0][]);
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException(
        "listNamespaces with a parent namespace is not supported");
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    String lanceNs = SparkLanceConverter.toLanceNamespace(namespace);
    if (!nsToPath.containsKey(lanceNs)) {
      throw new NoSuchNamespaceException(namespace);
    }
    return ImmutableMap.of(NS_PROPERTY_PATH, nsToPath.get(lanceNs));
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("listTables is not supported");
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    LanceConfig config = LanceConfig.from(options, ident.name());
    Optional<StructType> schema = LanceDatasetAdapter.getSchema(config);
    if (schema.isEmpty()) {
      throw new NoSuchTableException(config.getDbPath(), config.getDatasetName());
    }
    return new LanceDataset(config, schema.get());
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    try {
      LanceConfig config = LanceConfig.from(options, ident.name());
      WriteParams params = SparkOptions.genWriteParamsFromConfig(config);
      LanceDatasetAdapter.createDataset(ident.name(), schema, params);
    } catch (IllegalArgumentException e) {
      throw new TableAlreadyExistsException(ident.name(), new Some<>(e));
    }
    return new LanceDataset(LanceConfig.from(options, ident.name()), schema);
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    throw new UnsupportedOperationException("alterTable is not supported");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    LanceConfig config = LanceConfig.from(options, ident.name());
    LanceDatasetAdapter.dropDataset(config);
    return true;
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("renameTable is not supported");
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.name = name;
    this.options = options;
    this.nsToPath = PropertyUtils.propertiesWithPrefix(options, CATALOG_PROPERTY_PATHS_PREFIX);
    if (nsToPath.isEmpty()) {
      String path = PropertyUtils.propertyAsString(options, CATALOG_PROPERTY_PATH);
      this.nsToPath = ImmutableMap.of(DEFAULT_NAMESPACE, path);
    }
  }

  @Override
  public String[] defaultNamespace() {
    return new String[] {DEFAULT_NAMESPACE};
  }

  @Override
  public String name() {
    return name;
  }
}
