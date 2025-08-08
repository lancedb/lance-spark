# DataFrame Read

Load Lance tables as DataFrames for programmatic data access.

=== "Python"
    ```python
    # Load table as DataFrame
    users_df = spark.table("users")
    
    # Use DataFrame operations
    filtered_users = users_df.filter("age > 25").select("name", "email")
    filtered_users.show()
    ```

=== "Scala"
    ```scala
    // Load table as DataFrame
    val usersDF = spark.table("users")
    
    // Use DataFrame operations
    val filteredUsers = usersDF.filter("age > 25").select("name", "email")
    filteredUsers.show()
    ```

=== "Java"
    ```java
    // Load table as DataFrame
    Dataset<Row> usersDF = spark.table("users");
    
    // Use DataFrame operations
    Dataset<Row> filteredUsers = usersDF.filter("age > 25").select("name", "email");
    filteredUsers.show();
    ```