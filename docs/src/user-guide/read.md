# Reading Lance Datasets

## Basic Reading

=== "Python"
    ```python
    df = (spark.read
        .format("lance")
        .option("db", "/path/to/lance/database")
        .option("dataset", "my_dataset")
        .load())
    ```

=== "Scala"
    ```scala
    val df = spark.read.
        format("lance").
        option("db", "/path/to/lance/database").
        option("dataset", "my_dataset").
        load()
    ```

=== "Java"
    ```java
    Dataset<Row> df = spark.read()
        .format("lance")
        .option("db", "/path/to/lance/database")
        .option("dataset", "my_dataset")
        .load();
    ```

## Column Selection

Lance is a columnar format.
You can specify which columns to read to improve performance:

=== "Python"
    ```python
    df = (spark.read
        .format("lance")
        .option("db", "/path/to/lance/database")
        .option("dataset", "my_dataset")
        .load()
        .select("id", "name", "age"))
    ```

=== "Scala"
    ```scala
    val df = spark.read.
        format("lance").
        option("db", "/path/to/lance/database").
        option("dataset", "my_dataset").
        load().
        select("id", "name", "age")
    ```

=== "Java"
    ```java
    Dataset<Row> df = spark.read()
        .format("lance")
        .option("db", "/path/to/lance/database")
        .option("dataset", "my_dataset")
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
        .option("db", "/path/to/database")
        .option("dataset", "users")
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
        option("db", "/path/to/database").
        option("dataset", "users").
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
        .option("db", "/path/to/database")
        .option("dataset", "users")
        .load()
        .filter("age BETWEEN 25 AND 65 AND department = 'Engineering' AND is_active = true");
    ```