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
import com.lancedb.lance.catalog.client.apache.ApiClient;
import com.lancedb.lance.catalog.client.apache.ApiException;
import com.lancedb.lance.catalog.client.apache.Configuration;
import com.lancedb.lance.catalog.client.apache.api.DefaultApi;
import com.lancedb.lance.catalog.client.apache.api.NamespaceApi;
import com.lancedb.lance.catalog.client.apache.model.CreateNamespaceRequest;
import com.lancedb.lance.spark.internal.LanceDatasetAdapter;
import com.lancedb.lance.spark.utils.Optional;

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

public class LanceCatalog implements TableCatalog, SupportsNamespaces {
  private CaseInsensitiveStringMap options;

  private NamespaceApi namespaceApi;

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata) throws NamespaceAlreadyExistsException {
    CreateNamespaceRequest request = new CreateNamespaceRequest();
    request.setName(namespace[0]);
    String mode = metadata.getOrDefault("mode", "CREATE");
    request.setMode(CreateNamespaceRequest.ModeEnum.valueOf(mode));
    request.setOptions(metadata);
    try {
      namespaceApi.createNamespace(request);
    } catch (ApiException e) {
      if (e.getCode() == 400) {
        throw new NamespaceAlreadyExistsException(namespace);
      }
      throw new RuntimeException(e);
    }
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("alterNamespace is not supported");
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade) throws NoSuchNamespaceException, NonEmptyNamespaceException {
    throw new UnsupportedOperationException("dropNamespace is not supported");
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("listNamespaces is not supported");
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("listNamespaces is not supported");
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("loadNamespaceMetadata is not supported");
  }

  @Override
  public boolean namespaceExists(String[] namespace) {
    throw new UnsupportedOperationException("namespaceExists is not supported");
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("Please use lancedb catalog for dataset listing");
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
    throw new UnsupportedOperationException();
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
    throw new UnsupportedOperationException();
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.options = options;
    ApiClient defaultClient = Configuration.getDefaultApiClient();
    defaultClient.setBasePath("http://localhost:2333");
    this.namespaceApi = new NamespaceApi(defaultClient);
  }

  @Override
  public String name() {
    return "lance";
  }
}
