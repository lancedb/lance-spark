# Reading Lance Datasets

## Basic Reading

=== "Python"
    ```python
    # Method 1: Using path option
    df = (spark.read
        .format("lance")
        .option("path", "/path/to/lance/database/my_dataset.lance")
        .load())
    
    # Method 2: Direct path (alternative)
    df = spark.read.format("lance").load("/path/to/lance/database/my_dataset.lance")
    ```

=== "Scala"
    ```scala
    // Method 1: Using path option
    val df = spark.read.
        format("lance").
        option("path", "/path/to/lance/database/my_dataset.lance").
        load()
    
    // Method 2: Direct path (alternative)
    val df = spark.read.format("lance").load("/path/to/lance/database/my_dataset.lance")
    ```

=== "Java"
    ```java
    // Method 1: Using path option
    Dataset<Row> df = spark.read()
        .format("lance")
        .option("path", "/path/to/lance/database/my_dataset.lance")
        .load();
    
    // Method 2: Direct path (alternative)
    Dataset<Row> df = spark.read().format("lance").load("/path/to/lance/database/my_dataset.lance");
    ```

## Column Selection

Lance is a columnar format.
You can specify which columns to read to improve performance:

=== "Python"
    ```python
    df = (spark.read
        .format("lance")
        .option("path", "/path/to/lance/database/my_dataset.lance")
        .load()
        .select("id", "name", "age"))
    ```

=== "Scala"
    ```scala
    val df = spark.read.
        format("lance").
        option("path", "/path/to/lance/database/my_dataset.lance").
        load().
        select("id", "name", "age")
    ```

=== "Java"
    ```java
    Dataset<Row> df = spark.read()
        .format("lance")
        .option("path", "/path/to/lance/database/my_dataset.lance")
        .load()
        .select("id", "name", "age");
    ```

## Filters

You can apply filters to a read.
The filter is pushed down to reduce the amount of data read:

=== "Python"
    ```python
    from pyspark.sql.functions import col
    
    filtered = (spark.read
        .format("lance")
        .option("path", "/path/to/database/users.lance")
        .load()
        .filter(
            col("age").between(25, 65) &
            col("department") == "Engineering" &
            col("is_active") == True
        ))
    ```

=== "Scala"
    ```scala
    import org.apache.spark.sql.functions.col
    
    val filtered = spark.read.
        format("lance").
        option("path", "/path/to/database/users.lance").
        load().
        filter(
            col("age").between(25, 65) &&
            col("department") === "Engineering" &&
            col("is_active") === true
        )
    ```

=== "Java"
    ```java
    Dataset<Row> filtered = spark.read()
        .format("lance")
        .option("path", "/path/to/database/users.lance")
        .load()
        .filter("age BETWEEN 25 AND 65 AND department = 'Engineering' AND is_active = true");
    ```