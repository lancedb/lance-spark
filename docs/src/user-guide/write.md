# Writing Lance Datasets

## Basic Writing

=== "Python"
    ```python
    # Method 1: Using path option
    (df.write
        .format("lance")
        .option("path", "/path/to/lance/database/my_dataset.lance")
        .save())
    
    # Method 2: Direct path (alternative)
    (df.write
        .format("lance")
        .save("/path/to/lance/database/my_dataset.lance"))
    ```

=== "Scala"
    ```scala
    // Method 1: Using path option
    df.write.
        format("lance").
        option("path", "/path/to/lance/database/my_dataset.lance").
        save()
    
    // Method 2: Direct path (alternative)
    df.write.
        format("lance").
        save("/path/to/lance/database/my_dataset.lance")
    ```

=== "Java"
    ```java
    // Method 1: Using path option
    df.write()
        .format("lance")
        .option("path", "/path/to/lance/database/my_dataset.lance")
        .save();
    
    // Method 2: Direct path (alternative)
    df.write()
        .format("lance")
        .save("/path/to/lance/database/my_dataset.lance");
    ```

## Write Modes

### Create

By default, writing to a dataset at a specific path means creating the dataset:

=== "Python"
    ```python
    # First write - succeeds (Method 1: path option)
    (testData.write
        .format("lance")
        .option("path", "/path/to/database/users.lance")
        .save())
    
    # First write - succeeds (Method 2: direct path)
    (testData.write
        .format("lance")
        .save("/path/to/database/users.lance"))
    
    # Second write - throws TableAlreadyExistsException
    (testData.write
        .format("lance")
        .option("path", "/path/to/database/users.lance")
        .save())
    ```

=== "Scala"
    ```scala
    // First write - succeeds (Method 1: path option)
    testData.write.
        format("lance").
        option("path", "/path/to/database/users.lance").
        save()
    
    // First write - succeeds (Method 2: direct path)
    testData.write.
        format("lance").
        save("/path/to/database/users.lance")
    
    // Second write - throws TableAlreadyExistsException
    testData.write.
        format("lance").
        option("path", "/path/to/database/users.lance").
        save()
    ```

=== "Java"
    ```java
    // First write - succeeds (Method 1: path option)
    testData.write()
        .format("lance")
        .option("path", "/path/to/database/users.lance")
        .save();
    
    // First write - succeeds (Method 2: direct path)
    testData.write()
        .format("lance")
        .save("/path/to/database/users.lance");
    
    // Second write - throws TableAlreadyExistsException
    testData.write()
        .format("lance")
        .option("path", "/path/to/database/users.lance")
        .save();
    ```

### Append

Add new data to an existing dataset:

=== "Python"
    ```python
    # Create initial dataset
    (testData.write
        .format("lance")
        .option("path", "/path/to/database/users.lance")
        .save())
    
    # Append more data (Method 1: path option)
    (moreData.write
        .format("lance")
        .option("path", "/path/to/database/users.lance")
        .mode("append")
        .save())
    
    # Append more data (Method 2: direct path)
    (moreData.write
        .format("lance")
        .mode("append")
        .save("/path/to/database/users.lance"))
    ```

=== "Scala"
    ```scala
    // Create initial dataset
    testData.write.
        format("lance").
        option("path", "/path/to/database/users.lance").
        save()
    
    // Append more data (Method 1: path option)
    moreData.write.
        format("lance").
        option("path", "/path/to/database/users.lance").
        mode("append").
        save()
    
    // Append more data (Method 2: direct path)
    moreData.write.
        format("lance").
        mode("append").
        save("/path/to/database/users.lance")
    ```

=== "Java"
    ```java
    // Create initial dataset
    testData.write()
        .format("lance")
        .option("path", "/path/to/database/users.lance")
        .save();
    
    // Append more data (Method 1: path option)
    moreData.write()
        .format("lance")
        .option("path", "/path/to/database/users.lance")
        .mode("append")
        .save();
    
    // Append more data (Method 2: direct path)
    moreData.write()
        .format("lance")
        .mode("append")
        .save("/path/to/database/users.lance");
    ```

### Overwrite

Replace the entire dataset with new data:

=== "Python"
    ```python
    # Create initial dataset
    (initialData.write
        .format("lance")
        .option("path", "/path/to/database/users.lance")
        .save())
    
    # Completely replace the dataset (Method 1: path option)
    (newData.write
        .format("lance")
        .option("path", "/path/to/database/users.lance")
        .mode("overwrite")
        .save())
    
    # Completely replace the dataset (Method 2: direct path)
    (newData.write
        .format("lance")
        .mode("overwrite")
        .save("/path/to/database/users.lance"))
    ```

=== "Scala"
    ```scala
    // Create initial dataset
    initialData.write.
        format("lance").
        option("path", "/path/to/database/users.lance").
        save()
    
    // Completely replace the dataset (Method 1: path option)
    newData.write.
        format("lance").
        option("path", "/path/to/database/users.lance").
        mode("overwrite").
        save()
    
    // Completely replace the dataset (Method 2: direct path)
    newData.write.
        format("lance").
        mode("overwrite").
        save("/path/to/database/users.lance")
    ```

=== "Java"
    ```java
    // Create initial dataset
    initialData.write()
        .format("lance")
        .option("path", "/path/to/database/users.lance")
        .save();
    
    // Completely replace the dataset (Method 1: path option)
    newData.write()
        .format("lance")
        .option("path", "/path/to/database/users.lance")
        .mode("overwrite")
        .save();
    
    // Completely replace the dataset (Method 2: direct path)
    newData.write()
        .format("lance")
        .mode("overwrite")
        .save("/path/to/database/users.lance");
    ```
