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

import com.lancedb.lance.namespace.LanceNamespace;
import com.lancedb.lance.namespace.LanceNamespaces;
import com.lancedb.lance.namespace.ListTablesIterable;
import com.lancedb.lance.namespace.model.CreateTableRequest;
import com.lancedb.lance.namespace.model.CreateTableResponse;
import com.lancedb.lance.namespace.model.DescribeTableRequest;
import com.lancedb.lance.namespace.model.DescribeTableResponse;
import com.lancedb.lance.namespace.model.DropTableRequest;
import com.lancedb.lance.namespace.model.JsonArrowSchema;
import com.lancedb.lance.namespace.model.ListTablesRequest;
import com.lancedb.lance.namespace.util.JsonArrowSchemaConverter;
import com.lancedb.lance.namespace.util.PropertyUtil;
import com.lancedb.lance.spark.internal.LanceDatasetAdapter;
import com.lancedb.lance.spark.utils.Optional;
import com.lancedb.lance.spark.utils.SchemaConverter;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class LanceNamespaceSparkCatalog implements TableCatalog {

  private static final Logger logger = Logger.getLogger(LanceNamespaceSparkCatalog.class.getName());

  /** Used to specify the namespace implementation to use */
  private static final String CONFIG_IMPL = "impl";

  /**
   * Spark requires at least 3 levels of catalog -> namespaces -> table (most uses exactly 3 levels)
   * For namespaces that are only 2 levels (e.g. dir), this puts an extra dummy level namespace with
   * the given name, so that a namespace mounted as ns1 in Spark will have ns1 -> dummy_name ->
   * table structure.
   *
   * <p>For native implementations, we perform the following handling automatically:
   *
   * <ul>
   *   <li>dir: directly configure extra_level=default
   *   <li>rest: if ListNamespaces returns error, configure extra_level=default
   * </ul>
   */
  private static final String CONFIG_EXTRA_LEVEL = "extra_level";

  /** Supply in CREATE TABLE options to supply a different location to use for the table */
  private static final String CREATE_TABLE_PROPERTY_LOCATION = "location";

  private LanceNamespace namespace;
  private String name;
  private Optional<String> extraLevel;

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.name = name;

    if (!options.containsKey(CONFIG_IMPL)) {
      throw new IllegalArgumentException("Missing required configuration: " + CONFIG_IMPL);
    }
    String impl = PropertyUtil.propertyAsString(options, CONFIG_IMPL);

    // Initialize the namespace first
    this.namespace = LanceNamespaces.connect(impl, options, null, LanceDatasetAdapter.allocator);

    // Handle extra level name configuration
    if (options.containsKey(CONFIG_EXTRA_LEVEL)) {
      this.extraLevel = Optional.of(PropertyUtil.propertyAsString(options, CONFIG_EXTRA_LEVEL));
    } else if ("dir".equals(impl)) {
      this.extraLevel = Optional.of("default");
    } else if ("rest".equals(impl)) {
      this.extraLevel = determineExtraLevelForRest();
    } else {
      this.extraLevel = Optional.empty();
    }
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    String[] actualNamespace = removeExtraLevelsFromNamespace(namespace);

    ListTablesRequest request = new ListTablesRequest();
    request.setId(Arrays.stream(actualNamespace).collect(Collectors.toList()));
    ListTablesIterable tables = new ListTablesIterable(this.namespace, request);

    List<Identifier> identifiers = new ArrayList<>();
    for (String table : tables) {
      identifiers.add(Identifier.of(namespace, table));
    }

    return identifiers.toArray(new Identifier[0]);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    // Remove extra levels from the identifier
    Identifier actualIdent = removeExtraLevelsFromId(ident);

    DescribeTableRequest request = new DescribeTableRequest();
    for (String part : actualIdent.namespace()) {
      request.addIdItem(part);
    }
    request.addIdItem(actualIdent.name());
    try {
      DescribeTableResponse response = namespace.describeTable(request);
      String location = response.getLocation();
      if (location == null || location.isEmpty()) {
        throw new NoSuchTableException(ident);
      }

      LanceConfig config = LanceConfig.from(location);
      Optional<StructType> schema = LanceDatasetAdapter.getSchema(config);
      if (!schema.isPresent()) {
        throw new NoSuchTableException(ident);
      }

      return new LanceDataset(config, schema.get());
    } catch (Exception e) {
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    Identifier tableId = removeExtraLevelsFromId(ident);
    CreateTableRequest createRequest = new CreateTableRequest();
    for (String part : tableId.namespace()) {
      createRequest.addIdItem(part);
    }
    createRequest.addIdItem(tableId.name());

    if (properties != null && properties.containsKey(CREATE_TABLE_PROPERTY_LOCATION)) {
      createRequest.setLocation(properties.get(CREATE_TABLE_PROPERTY_LOCATION));
    }

    if (properties != null && !properties.isEmpty()) {
      createRequest.setProperties(properties);
    }

    JsonArrowSchema jsonSchema = SchemaConverter.toJsonArrowSchema(schema);
    createRequest.setSchema(jsonSchema);
    byte[] emptyArrowData = createEmptyArrowIpcStream(jsonSchema);
    CreateTableResponse response = namespace.createTable(createRequest, emptyArrowData);
    LanceConfig config = LanceConfig.from(response.getLocation());
    return new LanceDataset(config, schema);
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    throw new UnsupportedOperationException("Table alteration is not supported");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    try {
      Identifier tableId = removeExtraLevelsFromId(ident);
      DropTableRequest dropRequest = new DropTableRequest();
      for (String part : tableId.namespace()) {
        dropRequest.addIdItem(part);
      }
      dropRequest.addIdItem(tableId.name());
      namespace.dropTable(dropRequest);

      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    throw new UnsupportedOperationException("Table renaming is not supported");
  }

  /**
   * Removes the extra level from a Spark identifier if it matches the configured extra level name.
   * For example, with extraLevelName="default": - ["default", "table"] -> ["table"] - ["default"]
   * -> [] (root namespace) - ["other", "table"] -> ["other", "table"] (unchanged)
   */
  private Identifier removeExtraLevelsFromId(Identifier identifier) {
    if (extraLevel.isEmpty()) {
      return identifier;
    }

    String[] newNamespace = removeExtraLevelsFromNamespace(identifier.namespace());
    return Identifier.of(newNamespace, identifier.name());
  }

  /**
   * Removes the extra level from a namespace array if it matches the configured extra level name.
   * For example, with extraLevel="default": - ["default"] -> [] - ["default", "subnamespace"] ->
   * ["subnamespace"] - ["other"] -> ["other"] (unchanged)
   */
  private String[] removeExtraLevelsFromNamespace(String[] namespace) {
    if (extraLevel.isEmpty()) {
      return namespace;
    }

    String extraLevelName = this.extraLevel.get();

    // Check if the first namespace part matches the extra level
    if (namespace.length > 0 && extraLevelName.equals(namespace[0])) {
      // Remove the extra level from namespace
      String[] newNamespace = new String[namespace.length - 1];
      System.arraycopy(namespace, 1, newNamespace, 0, namespace.length - 1);
      return newNamespace;
    }

    return namespace;
  }

  /**
   * Creates an empty Arrow IPC stream with the given JsonArrowSchema. This is needed for REST
   * namespace implementations that require a body.
   *
   * @param jsonSchema The JsonArrowSchema
   * @return byte array containing empty Arrow IPC stream
   */
  private byte[] createEmptyArrowIpcStream(
      com.lancedb.lance.namespace.model.JsonArrowSchema jsonSchema) {
    try {
      // Convert JsonArrowSchema to Arrow Schema
      org.apache.arrow.vector.types.pojo.Schema arrowSchema =
          JsonArrowSchemaConverter.convertToArrowSchema(jsonSchema);

      // Create empty vector schema root
      BufferAllocator allocator = LanceDatasetAdapter.allocator;
      try (VectorSchemaRoot root = VectorSchemaRoot.create(arrowSchema, allocator);
          ByteArrayOutputStream out = new ByteArrayOutputStream();
          ArrowStreamWriter writer = new ArrowStreamWriter(root, null, out)) {

        writer.start();
        // Write empty batch (0 rows)
        root.setRowCount(0);
        writer.writeBatch();
        writer.end();

        return out.toByteArray();
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to create empty Arrow IPC stream", e);
    }
  }

  /**
   * Determines whether to use extra level for REST implementations by testing ListNamespaces. If
   * ListNamespaces fails, assumes flat namespace structure and returns "default".
   *
   * @return Optional containing "default" if ListNamespaces fails, empty otherwise
   */
  private Optional<String> determineExtraLevelForRest() {
    try {
      com.lancedb.lance.namespace.model.ListNamespacesRequest request =
          new com.lancedb.lance.namespace.model.ListNamespacesRequest();
      namespace.listNamespaces(request);
      return Optional.empty();
    } catch (Exception e) {
      logger.info(
          "REST namespace ListNamespaces failed, "
              + "falling back to flat table structure with extra_level=default");
      return Optional.of("default");
    }
  }
}
