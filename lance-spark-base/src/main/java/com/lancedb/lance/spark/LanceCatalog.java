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

import com.lancedb.lance.spark.utils.PropertyUtils;

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

import java.util.Map;

public class LanceCatalog implements TableCatalog, SupportsNamespaces {

  public static final String CATALOG_TYPE = "type";
  public static final String CATALOG_TYPE_DIR = "dir";
  public static final String CATALOG_TYPE_REST = "rest";

  private TableCatalog catalog;
  private SupportsNamespaces namespaces;

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    namespaces.createNamespace(namespace, metadata);
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    namespaces.alterNamespace(namespace, changes);
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    return namespaces.dropNamespace(namespace, cascade);
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return namespaces.listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return namespaces.listNamespaces(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    return namespaces.loadNamespaceMetadata(namespace);
  }

  @Override
  public boolean namespaceExists(String[] namespace) {
    return namespaces.namespaceExists(namespace);
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    return catalog.listTables(namespace);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return catalog.loadTable(ident);
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    return catalog.createTable(ident, schema, partitions, properties);
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    return catalog.alterTable(ident, changes);
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return catalog.dropTable(ident);
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    catalog.renameTable(oldIdent, newIdent);
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    String catalogType = PropertyUtils.propertyAsString(options, CATALOG_TYPE);
    if (catalogType.equals(CATALOG_TYPE_DIR)) {
      this.catalog = new LanceDirectories();
      catalog.initialize(name, options);
      this.namespaces = null;
    } else if (catalogType.equals(CATALOG_TYPE_REST)) {
      this.catalog = new LanceRestCatalog();
      catalog.initialize(name, options);
      this.namespaces = null;
    } else {
      throw new UnsupportedOperationException("Unknown catalog type: " + catalogType);
    }
  }

  @Override
  public String name() {
    return catalog.name();
  }
}
