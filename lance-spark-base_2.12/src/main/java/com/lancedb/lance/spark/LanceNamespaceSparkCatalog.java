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
import com.lancedb.lance.namespace.LanceNamespaceException;
import com.lancedb.lance.namespace.LanceNamespaces;
import com.lancedb.lance.namespace.ListTablesIterable;
import com.lancedb.lance.namespace.model.CreateTableRequest;
import com.lancedb.lance.namespace.model.CreateTableResponse;
import com.lancedb.lance.namespace.model.DescribeTableRequest;
import com.lancedb.lance.namespace.model.DescribeTableResponse;
import com.lancedb.lance.namespace.model.DropNamespaceRequest;
import com.lancedb.lance.namespace.model.DropTableRequest;
import com.lancedb.lance.namespace.model.JsonArrowSchema;
import com.lancedb.lance.namespace.model.ListTablesRequest;
import com.lancedb.lance.namespace.util.JsonArrowSchemaConverter;
import com.lancedb.lance.namespace.util.PropertyUtil;
import com.lancedb.lance.spark.internal.LanceDatasetAdapter;
import com.lancedb.lance.spark.utils.Optional;
import com.lancedb.lance.spark.utils.SchemaConverter;
import com.lancedb.lance.spark.utils.SparkUtil;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class LanceNamespaceSparkCatalog implements TableCatalog, SupportsNamespaces {

  private static final Logger logger = LoggerFactory.getLogger(LanceNamespaceSparkCatalog.class);

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

  /** Parent prefix configuration for multi-level namespaces like Hive3 */
  private static final String CONFIG_PARENT = "parent";

  private static final String CONFIG_PARENT_DELIMITER = "parent_delimiter";
  private static final String CONFIG_PARENT_DELIMITER_DEFAULT = ".";

  private LanceNamespace namespace;
  private String name;
  private Optional<String> extraLevel;
  private Optional<List<String>> parentPrefix;

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.name = name;

    if (!options.containsKey(CONFIG_IMPL)) {
      throw new IllegalArgumentException("Missing required configuration: " + CONFIG_IMPL);
    }
    String impl = PropertyUtil.propertyAsString(options, CONFIG_IMPL);

    // Handle parent prefix configuration
    if (options.containsKey(CONFIG_PARENT)) {
      String parent = PropertyUtil.propertyAsString(options, CONFIG_PARENT);
      String delimiter =
          PropertyUtil.propertyAsString(
              options, CONFIG_PARENT_DELIMITER, CONFIG_PARENT_DELIMITER_DEFAULT);
      List<String> parentParts = Arrays.asList(parent.split(Pattern.quote(delimiter)));
      this.parentPrefix = Optional.of(parentParts);
    } else {
      this.parentPrefix = Optional.empty();
    }

    // Initialize the namespace with proper configuration
    Map<String, String> namespaceOptions = new HashMap<>(options);
    Configuration hadoopConf = SparkUtil.hadoopConfCatalogOverrides(SparkSession.active(), name);

    this.namespace =
        LanceNamespaces.connect(impl, namespaceOptions, hadoopConf, LanceDatasetAdapter.allocator);

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
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    // Namespace alteration is not supported in the current API
    throw new UnsupportedOperationException("Namespace alteration is not supported");
  }

  @Override
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    // List root level namespaces
    com.lancedb.lance.namespace.model.ListNamespacesRequest request =
        new com.lancedb.lance.namespace.model.ListNamespacesRequest();

    // Add parent prefix to empty namespace (root)
    if (parentPrefix.isPresent()) {
      request.setId(parentPrefix.get());
    }

    try {
      com.lancedb.lance.namespace.model.ListNamespacesResponse response =
          namespace.listNamespaces(request);

      List<String[]> result = new ArrayList<>();
      for (String ns : response.getNamespaces()) {
        // For single-level namespace names, create array
        String[] nsArray = new String[] {ns};

        // Note: parent prefix removal is not needed here as the response
        // only contains the namespace name, not the full path

        // Add extra level if configured
        if (extraLevel.isPresent()) {
          String[] withExtra = new String[2];
          withExtra[0] = extraLevel.get();
          withExtra[1] = ns;
          nsArray = withExtra;
        }

        result.add(nsArray);
      }

      return result.toArray(new String[0][]);
    } catch (Exception e) {
      throw new NoSuchNamespaceException(new String[0]);
    }
  }

  @Override
  public String[][] listNamespaces(String[] parent) throws NoSuchNamespaceException {
    // Remove extra level and add parent prefix
    String[] actualParent = removeExtraLevelsFromNamespace(parent);
    actualParent = addParentPrefix(actualParent);

    com.lancedb.lance.namespace.model.ListNamespacesRequest request =
        new com.lancedb.lance.namespace.model.ListNamespacesRequest();
    request.setId(Arrays.asList(actualParent));

    try {
      com.lancedb.lance.namespace.model.ListNamespacesResponse response =
          namespace.listNamespaces(request);

      List<String[]> result = new ArrayList<>();
      for (String ns : response.getNamespaces()) {
        // For single-level namespace names, create full path
        String[] nsArray = new String[parent.length + 1];
        System.arraycopy(parent, 0, nsArray, 0, parent.length);
        nsArray[parent.length] = ns;

        result.add(nsArray);
      }

      return result.toArray(new String[0][]);
    } catch (Exception e) {
      throw new NoSuchNamespaceException(parent);
    }
  }

  @Override
  public boolean namespaceExists(String[] namespace) {
    // If the namespace is exactly the extra level, it exists as a virtual namespace
    if (extraLevel.isPresent() && namespace.length == 1 && extraLevel.get().equals(namespace[0])) {
      return true;
    }

    // Remove extra levels and add parent prefix
    String[] actualNamespace = removeExtraLevelsFromNamespace(namespace);
    actualNamespace = addParentPrefix(actualNamespace);

    com.lancedb.lance.namespace.model.NamespaceExistsRequest request =
        new com.lancedb.lance.namespace.model.NamespaceExistsRequest();
    request.setId(Arrays.asList(actualNamespace));

    try {
      this.namespace.namespaceExists(request);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {
    // Remove extra levels and add parent prefix
    String[] actualNamespace = removeExtraLevelsFromNamespace(namespace);
    actualNamespace = addParentPrefix(actualNamespace);

    com.lancedb.lance.namespace.model.DescribeNamespaceRequest request =
        new com.lancedb.lance.namespace.model.DescribeNamespaceRequest();
    request.setId(Arrays.asList(actualNamespace));

    try {
      com.lancedb.lance.namespace.model.DescribeNamespaceResponse response =
          this.namespace.describeNamespace(request);

      Map<String, String> properties = response.getProperties();
      return properties != null ? properties : Collections.emptyMap();
    } catch (Exception e) {
      throw new NoSuchNamespaceException(namespace);
    }
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> properties)
      throws NamespaceAlreadyExistsException {
    // Remove extra levels and add parent prefix
    String[] actualNamespace = removeExtraLevelsFromNamespace(namespace);
    actualNamespace = addParentPrefix(actualNamespace);

    com.lancedb.lance.namespace.model.CreateNamespaceRequest request =
        new com.lancedb.lance.namespace.model.CreateNamespaceRequest();
    request.setId(Arrays.asList(actualNamespace));

    if (properties != null && !properties.isEmpty()) {
      request.setProperties(properties);
    }

    try {
      this.namespace.createNamespace(request);
    } catch (Exception e) {
      if (e.getMessage() != null && e.getMessage().contains("already exists")) {
        throw new NamespaceAlreadyExistsException(namespace);
      }
      throw new RuntimeException("Failed to create namespace", e);
    }
  }

  @Override
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException {
    // Remove extra levels and add parent prefix
    String[] actualNamespace = removeExtraLevelsFromNamespace(namespace);
    actualNamespace = addParentPrefix(actualNamespace);

    DropNamespaceRequest request = new DropNamespaceRequest();
    request.setId(Arrays.asList(actualNamespace));

    // Set behavior based on cascade flag - let the Lance namespace API handle the logic
    if (cascade) {
      request.setBehavior(DropNamespaceRequest.BehaviorEnum.CASCADE);
    } else {
      request.setBehavior(DropNamespaceRequest.BehaviorEnum.RESTRICT);
    }

    this.namespace.dropNamespace(request);
    return true;
  }

  @Override
  public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
    String[] actualNamespace = removeExtraLevelsFromNamespace(namespace);
    actualNamespace = addParentPrefix(actualNamespace);

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
  public boolean tableExists(Identifier ident) {
    // Transform identifier for API call
    Identifier actualIdent = transformIdentifierForApi(ident);

    com.lancedb.lance.namespace.model.TableExistsRequest request =
        new com.lancedb.lance.namespace.model.TableExistsRequest();
    for (String part : actualIdent.namespace()) {
      request.addIdItem(part);
    }
    request.addIdItem(actualIdent.name());

    try {
      this.namespace.tableExists(request);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    // Transform identifier for API call
    Identifier actualIdent = transformIdentifierForApi(ident);

    DescribeTableRequest request = new DescribeTableRequest();
    for (String part : actualIdent.namespace()) {
      request.addIdItem(part);
    }
    request.addIdItem(actualIdent.name());

    DescribeTableResponse response;
    try {
      response = namespace.describeTable(request);
    } catch (LanceNamespaceException e) {
      if (e.getCode() == 404) {
        throw new NoSuchTableException(ident);
      }
      throw e;
    }

    // Pass storage options from the response to LanceConfig, with fallback to empty map
    Map<String, String> storageOptions = response.getStorageOptions();
    if (storageOptions == null) {
      storageOptions = new HashMap<>();
    }

    LanceConfig config = LanceConfig.from(storageOptions, response.getLocation());
    Optional<StructType> schema = LanceDatasetAdapter.getSchema(config);
    if (!schema.isPresent()) {
      throw new NoSuchTableException(ident);
    }

    return new LanceDataset(config, schema.get());
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    Identifier tableId = transformIdentifierForApi(ident);
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

    // Pass storage options from the response to LanceConfig, with fallback to empty map
    Map<String, String> storageOptions = response.getStorageOptions();
    if (storageOptions == null) {
      storageOptions = new HashMap<>();
    }

    LanceConfig config = LanceConfig.from(storageOptions, response.getLocation());
    return new LanceDataset(config, schema);
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    throw new UnsupportedOperationException("Table alteration is not supported");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    try {
      Identifier tableId = transformIdentifierForApi(ident);
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

  /** Transforms an identifier for API calls by removing extra levels and adding parent prefix. */
  private Identifier transformIdentifierForApi(Identifier identifier) {
    Identifier transformed = removeExtraLevelsFromId(identifier);
    String[] namespace = addParentPrefix(transformed.namespace());
    return Identifier.of(namespace, transformed.name());
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

  /**
   * Adds parent prefix to namespace array for API calls. For example, with
   * parentPrefix=["catalog1"], ["ns1"] becomes ["catalog1", "ns1"]
   */
  private String[] addParentPrefix(String[] namespace) {
    if (parentPrefix.isEmpty()) {
      return namespace;
    }

    List<String> result = new ArrayList<>(parentPrefix.get());
    result.addAll(Arrays.asList(namespace));
    return result.toArray(new String[0]);
  }

  /**
   * Removes parent prefix from namespace array for Spark. For example, with
   * parentPrefix=["catalog1"], ["catalog1", "ns1"] becomes ["ns1"]
   */
  private String[] removeParentPrefix(String[] namespace) {
    if (parentPrefix.isEmpty()) {
      return namespace;
    }

    List<String> prefix = parentPrefix.get();
    if (namespace.length >= prefix.size()) {
      // Check if namespace starts with the parent prefix
      boolean hasPrefix = true;
      for (int i = 0; i < prefix.size(); i++) {
        if (!prefix.get(i).equals(namespace[i])) {
          hasPrefix = false;
          break;
        }
      }

      if (hasPrefix) {
        // Remove the prefix
        String[] result = new String[namespace.length - prefix.size()];
        System.arraycopy(namespace, prefix.size(), result, 0, result.length);
        return result;
      }
    }

    return namespace;
  }
}
