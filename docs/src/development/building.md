# Building from Source

This guide covers how to build the Apache Spark Connector for Lance from source code.

## Prerequisites

Before building from source, ensure you have the following installed:

### Required Software

- **Java**: JDK 8, 11, or 17
- **Maven**: 3.6.0 or higher
- **Git**: For cloning repositories

### Verify Installation

```bash
# Check Java version
java -version

# Check Maven version
mvn -version

# Check Git version
git --version
```

## Dependencies

### Lance Java SDK

This package depends on the [Lance Java SDK](https://github.com/lancedb/lance/blob/main/java) and [Lance Catalog Java Modules](https://github.com/lancedb/lance-catalog/tree/main/java).

!!! warning "Important"
    You need to build those repositories locally first before building this repository. If you have changes affecting those repositories, the PR in `lancedb/lance-spark` will only pass CI after the PRs in `lancedb/lance` and `lancedb/lance-catalog` are merged.

### Building Dependencies

1. **Clone and build Lance Java SDK**:
   ```bash
   git clone https://github.com/lancedb/lance.git
   cd lance/java
   mvn clean install
   cd ../..
   ```

2. **Clone and build Lance Catalog**:
   ```bash
   git clone https://github.com/lancedb/lance-catalog.git
   cd lance-catalog/java
   mvn clean install
   cd ../..
   ```

## Cloning the Repository

Clone the lance-spark repository:

```bash
git clone https://github.com/lancedb/lance-spark.git
cd lance-spark
```

## Basic Build Commands

### Build Everything

To build all modules with tests:

```bash
./mvnw clean install
```

### Build Without Tests

To build everything without running tests (faster):

```bash
./mvnw clean install -DskipTests
```

### Clean Build

To clean all previous build artifacts:

```bash
./mvnw clean
```

## Build Profiles

The project supports multiple build profiles for different versions:

### Scala Profiles

- `scala-2.12` (default)
- `scala-2.13`

### Spark Profiles

- `spark-3.4`
- `spark-3.5` (default)

### Using Profiles

#### Build with Scala 2.13

```bash
./mvnw clean install -Pscala-2.13
```

#### Build with Spark 3.4

```bash
./mvnw clean install -Pspark-3.4
```

#### Combine Profiles

```bash
./mvnw clean install -Pscala-2.13,spark-3.4
```

## Building Specific Modules

### Build Only Spark 3.4 Module

```bash
./mvnw clean install -Pspark-3.4 -pl lance-spark-3.4_2.12 -am
```

### Build Only Spark 3.5 Module

```bash
./mvnw clean install -Pspark-3.5 -pl lance-spark-3.5_2.12 -am
```

### Build Base Module

```bash
./mvnw clean install -pl lance-spark-base_2.12 -am
```

## Creating Distribution JARs

### Bundled JAR (Recommended)

To create a JAR with all dependencies for Spark 3.5:

```bash
./mvnw clean install -Pspark-3.5 -Pshade-jar -pl lance-spark-bundle-3.5_2.12 -am
```

### Lean JAR

To create a JAR with only the connector code:

```bash
./mvnw clean install -Pspark-3.5 -pl lance-spark-3.5_2.12 -am
```

## Build Output

After a successful build, you'll find the JARs in the following locations:

```
target/
├── lance-spark-base_2.12-<version>.jar
├── lance-spark-3.5_2.12-<version>.jar
├── lance-spark-bundle-3.5_2.12-<version>.jar
└── ...
```

## Development Workflow

### Typical Development Cycle

1. **Make changes** to the source code
2. **Run tests** to verify changes:
   ```bash
   ./mvnw test
   ```
3. **Build locally** to check compilation:
   ```bash
   ./mvnw clean install -DskipTests
   ```
4. **Run integration tests**:
   ```bash
   ./mvnw verify
   ```

### Testing Specific Modules

```bash
# Test only the base module
./mvnw test -pl lance-spark-base_2.12

# Test only Spark 3.5 module
./mvnw test -pl lance-spark-3.5_2.12
```

## Build Optimization

### Parallel Builds

Enable parallel builds for faster compilation:

```bash
./mvnw clean install -T 4  # Use 4 threads
./mvnw clean install -T 1C # Use 1 thread per CPU core
```

### Offline Builds

If you have all dependencies cached:

```bash
./mvnw clean install -o
```

### Skip Specific Phases

```bash
# Skip tests
./mvnw clean install -DskipTests

# Skip integration tests only
./mvnw clean install -DskipITs

# Skip checkstyle
./mvnw clean install -Dcheckstyle.skip=true
```

## Troubleshooting Build Issues

### Common Problems

#### 1. Lance Java SDK Not Found

**Error**: `Could not find artifact com.lancedb:lance-java`

**Solution**: Build Lance Java SDK first:
```bash
cd /path/to/lance/java
mvn clean install
```

#### 2. Out of Memory During Build

**Error**: `java.lang.OutOfMemoryError`

**Solution**: Increase Maven memory:
```bash
export MAVEN_OPTS="-Xmx4g -XX:MaxPermSize=512m"
./mvnw clean install
```

#### 3. Test Failures

**Error**: Tests failing due to missing test data

**Solution**: Ensure test resources are available:
```bash
./mvnw clean test-compile
./mvnw test
```

### Build Environment Issues

#### Java Version Conflicts

Ensure you're using a supported Java version:

```bash
# Check current Java version
java -version

# Set JAVA_HOME if needed
export JAVA_HOME=/path/to/java/11
```

#### Maven Version Issues

Ensure Maven version is 3.6.0 or higher:

```bash
mvn -version
```

## IDE Setup

### IntelliJ IDEA

1. **Import Project**: File → Open → Select `pom.xml`
2. **Configure JDK**: File → Project Structure → Project → Project SDK
3. **Enable Profiles**: View → Tool Windows → Maven → Profiles

### Eclipse

1. **Import Project**: File → Import → Existing Maven Projects
2. **Configure JDK**: Project Properties → Java Build Path
3. **Enable Profiles**: Right-click project → Maven → Select Maven Profiles

### VS Code

1. **Install Extensions**: Java Extension Pack, Maven for Java
2. **Open Folder**: Open the lance-spark directory
3. **Configure Java**: Ctrl+Shift+P → Java: Configure Runtime

## Continuous Integration

The project uses GitHub Actions for CI. The build matrix includes:

- Java versions: 8, 11, 17
- Scala versions: 2.12, 2.13
- Spark versions: 3.4, 3.5
- Operating systems: Ubuntu, macOS, Windows

### Local CI Simulation

To simulate CI builds locally:

```bash
# Test multiple Java versions (if available)
JAVA_HOME=/path/to/java/8 ./mvnw clean test
JAVA_HOME=/path/to/java/11 ./mvnw clean test
JAVA_HOME=/path/to/java/17 ./mvnw clean test

# Test different profiles
./mvnw clean test -Pscala-2.12,spark-3.5
./mvnw clean test -Pscala-2.13,spark-3.4
```

## Next Steps

- [Multi-Version Support](multi-version.md) - Learn about supporting multiple Spark/Scala versions
- [Testing](testing.md) - Run and write tests
- [Contributing](contributing.md) - Contribute to the project 