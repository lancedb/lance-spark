# Inspecting your Lance Datasets

## SELECT

```sql
-- Select all data from a table
SELECT * FROM users;

-- Select specific columns
SELECT id, name, email FROM users;

-- Query with WHERE clause
SELECT * FROM users WHERE age > 25;

-- Aggregate queries
SELECT department, COUNT(*) as employee_count 
FROM users 
GROUP BY department;

-- Join queries
SELECT u.name, p.title
FROM users u
JOIN projects p ON u.id = p.user_id;
```

## SHOW TABLES

```sql
-- Show all tables in the default namespace
SHOW TABLES;
     
-- Show all tables in a specific namespace ns2
 SNOW TABLES IN ns2;
```

## DESCRIBE TABLE

```sql

-- Describe table structure
DESCRIBE TABLE users;

-- Show detailed table information
DESCRIBE EXTENDED users;
```

## DataFrame Read

=== "Python"
    ```python
    # Load table as DataFrame
    users_df = spark.table("lance.default.users")
    
    # Use DataFrame operations
    filtered_users = users_df.filter("age > 25").select("name", "email")
    filtered_users.show()
    ```

=== "Scala"
    ```scala
    // Load table as DataFrame
    val usersDF = spark.table("lance.default.users")
    
    // Use DataFrame operations
    val filteredUsers = usersDF.filter("age > 25").select("name", "email")
    filteredUsers.show()
    ```

=== "Java"
    ```java
    // Load table as DataFrame
    Dataset<Row> usersDF = spark.table("lance.default.users");
    
    // Use DataFrame operations
    Dataset<Row> filteredUsers = usersDF.filter("age > 25").select("name", "email");
    filteredUsers.show();
    ```