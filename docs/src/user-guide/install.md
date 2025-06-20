# Install

## Maven Central Packages

The connector packages are published to Maven Central under the `com.lancedb` namespace. Choose the appropriate artifact based on your use case:

### Available Artifacts

| Artifact Type | Name Pattern                                         | Description                                                            | Example                     |
|---------------|------------------------------------------------------|------------------------------------------------------------------------|-----------------------------|
| Base Jar      | `lance-spark-base_<scala_version>`                   | Jar with logic shared by different versions of Spark Lance connectors. | lance-spark-base_2.12       |
| Lean Jar      | `lance-spark-<spark-version>_<scala_version>`        | Jar with only the Spark Lance connector logic                          | lance-spark-3.5_2.12        |
| Bundled Jar   | `lance-spark-bundle-<spark-version>_<scala_version>` | Jar with all necessary non-Spark dependencies                          | lance-spark-bundle-3.5_2.12 |

### Choosing the Right Artifact

- **Bundled Jar**: Recommended for most users. Use this if you want to quickly get started or use the connector in a Spark shell/notebook environment.
- **Lean Jar**: Use this if you're building a custom Spark application and want to manage and bundle dependencies yourself.
- **Base Jar**: Internal use only. Use this if you would like to build a custom Spark Lance connector with a different Spark version.

## Dependency Configuration

### In Spark Application Code

Typically, you use the bundled jar in your Spark application as a provided (compile only) dependency.
The actual jar is supplied to the Spark cluster separately.
If you want to also include the bundled jar in your own bundle, remove the provided (compile only) annotation.

=== "Maven"
    ```xml
    <dependency>
        <groupId>com.lancedb.lance</groupId>
        <artifactId>lance-spark-bundle-3.5_2.12</artifactId>
        <version>0.0.1</version>
        <scope>provided</scope>
    </dependency>
    ```

=== "Gradle"
    ```gradle   
    dependencies {
        compileOnly 'com.lancedb.lance:lance-spark-bundle-3.5_2.12:0.0.1'
    }
    ```

=== "sbt"
    ```scala
    libraryDependencies += "com.lancedb.lance" %% "lance-spark-bundle-3.5_2.12" % "0.0.1" % "provided"
    ```

### In Spark Cluster

You can either download the bundled jar dependency from Maven and add it to your Spark classpath,
or supply the dependency dynamically to a Spark cluster through `--packages` flag.
See [Spark Catalog Configuration](config.md#spark-catalog-configuration) for more details.

## Requirements

### Java

| Java Version | Support Status | Notes                              |
|--------------|----------------|------------------------------------|
| Java 8       | ✅ Supported    | Minimum required version           |
| Java 11      | ✅ Supported    | Recommended for production         |
| Java 17      | ✅ Supported    | Latest LTS version                 |
| Java 21+     | ⚠️ Untested    | May work but not officially tested |

### Scala

| Scala Version | Support Status  | Notes                               |
|---------------|-----------------|-------------------------------------|
| Scala 2.12    | ✅ Supported     | Required                            |
| Scala 2.13    | 🚧 In Progress  | Support planned for future releases |
| Scala 3.x     | ❌ Not Supported | Not currently planned               |

### Apache Spark

| Spark Version       | Support Status  | Notes                                                        |
|---------------------|-----------------|--------------------------------------------------------------|
| Spark 3.5           | ✅ Supported     | Primary supported version                                    |
| Spark 3.4           | ✅ Supported     | Requires manual building, Maven publication work in progress |
| Spark 3.1, 3.2, 3.3 | ⚠️ Untested     | May work but not officially tested                           |
| Spark 4.0           | 🚧 In Progress  | Support planned for future releases                          |
| Spark 2.x           | ❌ Not Supported |                                                              |

### Operating System

| Operating System | Architecture | Support Status | Notes                               |
|------------------|--------------|----------------|-------------------------------------|
| Linux            | x86_64       | ✅ Supported    |                                     |
| Linux            | ARM64        | ✅ Supported    | Including Apple Silicon via Rosetta |
| macOS            | x86_64       | ✅ Supported    | Intel-based Macs                    |
| macOS            | ARM64        | ✅ Supported    | Apple Silicon (M1/M2/M3)            |
| Windows          | x86_64       | 🚧 In Progress | Support planned for future releases |
