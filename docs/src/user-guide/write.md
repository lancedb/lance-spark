# Managing your Lance Datasets

## CREATE TABLE

```sql
-- Create a simple table
CREATE TABLE users (
    id BIGINT NOT NULL,
    name STRING,
    email STRING,
    created_at TIMESTAMP
);


-- Create table with complex data types
CREATE TABLE events (
    event_id BIGINT NOT NULL,
    user_id BIGINT,
    event_type STRING,
    tags ARRAY<STRING>,
    metadata STRUCT<
        source: STRING,
        version: INT,
        processed_at: TIMESTAMP
    >,
    occurred_at TIMESTAMP
);
```

## INSERT INTO

```sql
-- Insert individual rows
INSERT INTO users VALUES 
    (4, 'David', 'david@example.com', '2024-01-15 10:30:00'),
    (5, 'Eva', 'eva@example.com', '2024-01-15 11:45:00');

-- Insert with column specification
INSERT INTO users (id, name, email) VALUES 
    (6, 'Frank', 'frank@example.com'),
    (7, 'Grace', 'grace@example.com');

-- Insert from SELECT query
INSERT INTO users
SELECT user_id as id, username as name, email_address as email, signup_date as created_at
FROM staging.user_signups
WHERE signup_date >= '2024-01-01';

-- Insert with complex data types
INSERT INTO events VALUES (
    1001,
    123,
    'page_view',
    map('page', '/home', 'referrer', 'google'),
    array('web', 'desktop'),
    struct('web_app', 1, '2024-01-15 12:00:00'),
    '2024-01-15 12:00:00'
);
```

## DROP TABLE

```sql
-- Drop table
DROP TABLE users;

-- Drop table if it exists (no error if table doesn't exist)
DROP TABLE IF EXISTS users;
```

## DataFrame CreateTable

=== "Python"
    ```python
    # Create DataFrame
    data = [
    (1, "Alice", "alice@example.com"),
    (2, "Bob", "bob@example.com"),
    (3, "Charlie", "charlie@example.com")
    ]
    df = spark.createDataFrame(data, ["id", "name", "email"])
    
    # Write as new table using catalog
    df.writeTo("users").create()
    ```

=== "Scala"
    ```scala
    import spark.implicits._
    
    // Create DataFrame
    val data = Seq(
        (1, "Alice", "alice@example.com"),
        (2, "Bob", "bob@example.com"),
        (3, "Charlie", "charlie@example.com")
    )
    val df = data.toDF("id", "name", "email")
    
    // Write as new table using catalog
    df.writeTo("users").create()
    ```

=== "Java"
    ```java
    import org.apache.spark.sql.types.*;
    import org.apache.spark.sql.Row;
    import org.apache.spark.sql.RowFactory;
    
    // Create DataFrame
    List<Row> data = Arrays.asList(
        RowFactory.create(1L, "Alice", "alice@example.com"),
        RowFactory.create(2L, "Bob", "bob@example.com"),
        RowFactory.create(3L, "Charlie", "charlie@example.com")
    );
    
    StructType schema = new StructType(new StructField[]{
        new StructField("id", DataTypes.LongType, false, Metadata.empty()),
        new StructField("name", DataTypes.StringType, true, Metadata.empty()),
        new StructField("email", DataTypes.StringType, true, Metadata.empty())
    });
    
    Dataset<Row> df = spark.createDataFrame(data, schema);
    
    // Write as new table using catalog
    df.writeTo("users").create();
    ```

## DataFrame Write

=== "Python"
    ```python
    # Create new data
    new_data = [
        (8, "Henry", "henry@example.com"),
        (9, "Ivy", "ivy@example.com")
    ]
    new_df = spark.createDataFrame(new_data, ["id", "name", "email"])
    
    # Append to existing table
    new_df.writeTo("users").append()
    
    # Alternative: use traditional write API with mode
    new_df.write.mode("append").saveAsTable("users")
    ```

=== "Scala"
    ```scala
    // Create new data
    val newData = Seq(
        (8, "Henry", "henry@example.com"),
        (9, "Ivy", "ivy@example.com")
    )
    val newDF = newData.toDF("id", "name", "email")
    
    // Append to existing table
    newDF.writeTo("users").append()
    
    // Alternative: use traditional write API with mode
    newDF.write.mode("append").saveAsTable("users")
    ```

=== "Java"
    ```java
    // Create new data
    List<Row> newData = Arrays.asList(
        RowFactory.create(8L, "Henry", "henry@example.com"),
        RowFactory.create(9L, "Ivy", "ivy@example.com")
    );
    Dataset<Row> newDF = spark.createDataFrame(newData, schema);
    
    // Append to existing table
    newDF.writeTo("users").append();
    
    // Alternative: use traditional write API with mode
    newDF.write().mode("append").saveAsTable("users");
    ```
