# Writing Lance Datasets

## Basic Writing

=== "Python"
    ```python
    (df.write
        .format("lance")
        .option("dataset_uri", "/path/to/lance/database/my_dataset")
        .save())
    ```

=== "Scala"
    ```scala
    df.write.
        format("lance").
        option("dataset_uri", "/path/to/lance/database/my_dataset").
        save()
    ```

=== "Java"
    ```java
    df.write()
        .format("lance")
        .option("dataset_uri", "/path/to/lance/database/my_dataset")
        .save();
    ```

Alternatively, you can specify the path directly in the `save()` method:

=== "Python"
    ```python
    (df.write
        .format("lance")
        .save("/path/to/lance/database/my_dataset"))
    ```

=== "Scala"
    ```scala
    df.write.
        format("lance").
        save("/path/to/lance/database/my_dataset")
    ```

=== "Java"
    ```java
    df.write()
        .format("lance")
        .save("/path/to/lance/database/my_dataset");
    ```

## Write Modes

### Create

By default, writing to a dataset at a specific path means creating the dataset:

=== "Python"
    ```python
    # First write - succeeds
    (testData.write
        .format("lance")
        .option("dataset_uri", "/path/to/database/users")
        .save())
    
    # Second write - throws TableAlreadyExistsException
    (testData.write
        .format("lance")
        .option("dataset_uri", "/path/to/database/users")
        .save())
    ```

=== "Scala"
    ```scala
    // First write - succeeds
    testData.write.
        format("lance").
        option("dataset_uri", "/path/to/database/users").
        save()
    
    // Second write - throws TableAlreadyExistsException
    testData.write.
        format("lance").
        option("dataset_uri", "/path/to/database/users").
        save()
    ```

=== "Java"
    ```java
    // First write - succeeds
    testData.write()
        .format("lance")
        .option("dataset_uri", "/path/to/database/users")
        .save();
    
    // Second write - throws TableAlreadyExistsException
    testData.write()
        .format("lance")
        .option("dataset_uri", "/path/to/database/users")
        .save();
    ```

### Append

Add new data to an existing dataset:

=== "Python"
    ```python
    # Create initial dataset
    (testData.write
        .format("lance")
        .option("dataset_uri", "/path/to/database/users")
        .save())
    
    # Append more data
    (moreData.write
        .format("lance")
        .option("dataset_uri", "/path/to/database/users")
        .mode("append")
        .save())
    ```

=== "Scala"
    ```scala
    // Create initial dataset
    testData.write.
        format("lance").
        option("dataset_uri", "/path/to/database/users").
        save()
    
    // Append more data
    moreData.write.
        format("lance").
        option("dataset_uri", "/path/to/database/users").
        mode("append").
        save()
    ```

=== "Java"
    ```java
    // Create initial dataset
    testData.write()
        .format("lance")
        .option("dataset_uri", "/path/to/database/users")
        .save();
    
    // Append more data
    moreData.write()
        .format("lance")
        .option("dataset_uri", "/path/to/database/users")
        .mode("append")
        .save();
    ```

### Overwrite

Replace the entire dataset with new data:

=== "Python"
    ```python
    # Create initial dataset
    (initialData.write
        .format("lance")
        .option("dataset_uri", "/path/to/database/users")
        .save())
    
    # Completely replace the dataset
    (newData.write
        .format("lance")
        .option("dataset_uri", "/path/to/database/users")
        .mode("overwrite")
        .save())
    ```

=== "Scala"
    ```scala
    // Create initial dataset
    initialData.write.
        format("lance").
        option("dataset_uri", "/path/to/database/users").
        save()
    
    // Completely replace the dataset
    newData.write.
        format("lance").
        option("dataset_uri", "/path/to/database/users").
        mode("overwrite").
        save()
    ```

=== "Java"
    ```java
    // Create initial dataset
    initialData.write()
        .format("lance")
        .option("dataset_uri", "/path/to/database/users")
        .save();
    
    // Completely replace the dataset
    newData.write()
        .format("lance")
        .option("dataset_uri", "/path/to/database/users")
        .mode("overwrite")
        .save();
    ```
