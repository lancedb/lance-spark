# Requirements

This page details the system requirements and compatibility information for the Apache Spark Connector for Lance.

## System Requirements

### Java

The connector requires Java 8 or higher:

| Java Version | Support Status | Notes |
|--------------|----------------|-------|
| Java 8       | ✅ Supported   | Minimum required version |
| Java 11      | ✅ Supported   | Recommended for production |
| Java 17      | ✅ Supported   | Latest LTS version |
| Java 21+     | ⚠️ Untested    | May work but not officially tested |

### Scala

Currently, only Scala 2.12 is supported:

| Scala Version | Support Status | Notes |
|---------------|----------------|-------|
| Scala 2.12    | ✅ Supported   | Required |
| Scala 2.13    | 🚧 In Progress | Support planned for future releases |
| Scala 3.x     | ❌ Not Supported | Not currently planned |

### Apache Spark

The connector is built for Apache Spark 3.5:

| Spark Version | Support Status | Notes |
|---------------|----------------|-------|
| Spark 3.4     | 🚧 In Progress | Support available in development |
| Spark 3.5     | ✅ Supported   | Primary supported version |
| Spark 3.6+    | ⚠️ Untested    | May work but not officially tested |
| Spark 2.x     | ❌ Not Supported | Legacy versions not supported |

## Operating System Support

The connector supports any operating system that is compatible with the Lance Java SDK:

### Supported Platforms

| Operating System | Architecture | Support Status | Notes |
|------------------|--------------|----------------|-------|
| Linux            | x86_64       | ✅ Supported   | Primary development platform |
| Linux            | ARM64        | ✅ Supported   | Including Apple Silicon via Rosetta |
| macOS            | x86_64       | ✅ Supported   | Intel-based Macs |
| macOS            | ARM64        | ✅ Supported   | Apple Silicon (M1/M2/M3) |
| Windows          | x86_64       | ✅ Supported   | Windows 10/11 |

### Container Support

The connector works in containerized environments:

- **Docker**: Fully supported
- **Kubernetes**: Fully supported
- **Cloud platforms**: AWS EMR, Azure HDInsight, Google Dataproc

## Memory Requirements

### Minimum Requirements

- **Heap Memory**: 2GB minimum for basic operations
- **Off-heap Memory**: Additional memory for Arrow operations

### Recommended Configuration

For production workloads:

```bash
# Spark configuration
spark.executor.memory=4g
spark.executor.memoryFraction=0.8
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
```

## Dependencies

### Lance Java SDK

The connector depends on the Lance Java SDK, which provides:

- Native libraries for Lance format operations
- Arrow integration for columnar data processing
- Memory-mapped file access for performance

### Automatic Dependency Management

When using the bundled JAR (`lance-spark-bundle-*`), all dependencies are included:

- Lance Java SDK
- Apache Arrow Java
- Required native libraries

### Manual Dependency Management

If using the lean JAR (`lance-spark-*`), you need to manage these dependencies:

```xml
<dependencies>
    <dependency>
        <groupId>com.lancedb.lance</groupId>
        <artifactId>lance-spark-3.5_2.12</artifactId>
        <version>0.0.1</version>
    </dependency>
    <dependency>
        <groupId>com.lancedb</groupId>
        <artifactId>lance-java</artifactId>
        <version>0.0.1</version>
    </dependency>
</dependencies>
```

## Network Requirements

### Maven Central Access

The connector is distributed via Maven Central, so your build environment needs:

- Internet access to download dependencies
- Access to `repo1.maven.org` and `central.maven.org`

### Firewall Considerations

If running in a restricted network environment, ensure access to:

- Maven Central repositories
- Lance dataset storage locations (local filesystem, S3, etc.)

## Performance Considerations

### CPU Requirements

- **Minimum**: 2 CPU cores
- **Recommended**: 4+ CPU cores for parallel processing

### Storage Requirements

- **Local disk**: For temporary files and caching
- **Network storage**: For distributed Lance datasets

### Network Bandwidth

For remote Lance datasets:

- **Minimum**: 100 Mbps for basic operations
- **Recommended**: 1 Gbps+ for high-throughput workloads

## Compatibility Matrix

### Tested Combinations

| Spark | Scala | Java | OS | Status |
|-------|-------|------|----|---------| 
| 3.5.0 | 2.12  | 8    | Linux x86_64 | ✅ Tested |
| 3.5.0 | 2.12  | 11   | Linux x86_64 | ✅ Tested |
| 3.5.0 | 2.12  | 17   | Linux x86_64 | ✅ Tested |
| 3.5.0 | 2.12  | 11   | macOS ARM64  | ✅ Tested |
| 3.5.0 | 2.12  | 11   | Windows x86_64 | ✅ Tested |

## Troubleshooting Requirements Issues

### Common Issues

1. **UnsatisfiedLinkError**: Native library not found
   - Ensure your OS/architecture is supported
   - Check that the bundled JAR is being used

2. **ClassNotFoundException**: Connector classes not found
   - Verify the correct JAR is in the classpath
   - Check Spark and Scala version compatibility

3. **OutOfMemoryError**: Insufficient memory
   - Increase executor memory
   - Tune memory fractions

### Getting Help

If you encounter compatibility issues:

1. Check the [Troubleshooting Guide](../reference/troubleshooting.md)
2. Review the [GitHub Issues](https://github.com/lancedb/lance-spark/issues)
3. Join our [Discord community](https://discord.gg/zMM32dvNtd)

## Next Steps

- [Installation Guide](installation.md) - Install the connector
- [Quick Start](quick-start.md) - Get started with basic usage
- [Configuration](../user-guide/configuration.md) - Advanced configuration options 