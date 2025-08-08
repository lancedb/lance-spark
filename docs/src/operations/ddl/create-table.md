# CREATE TABLE

Create new Lance tables with SQL DDL statements.

## Basic Table Creation

Create a simple table:

```sql
CREATE TABLE users (
    id BIGINT NOT NULL,
    name STRING,
    email STRING,
    created_at TIMESTAMP
);
```

Create table with complex data types:

```sql
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

## Vector Columns

Lance supports vector columns for machine learning workloads (embeddings, similarity search). You can create tables with vector columns by using `ARRAY<FLOAT>` or `ARRAY<DOUBLE>` types with table properties specifying the fixed dimension.

### Supported Types

- **Element Types**: `FLOAT` (float32), `DOUBLE` (float64)
- **Requirements**:
  - Vectors must be non-nullable
  - All vectors in a column must have the same dimension
  - Dimension is specified via table properties

### Creating Vector Columns

Create a table with a vector column:

```sql
CREATE TABLE embeddings_table (
    id INT NOT NULL,
    text STRING,
    embeddings ARRAY<FLOAT> NOT NULL
) USING lance
TBLPROPERTIES (
    'embeddings.arrow.fixed-size-list.size' = '128'
);
```

Create table with multiple vector columns of different dimensions:

```sql
CREATE TABLE multi_vector_table (
    id INT NOT NULL,
    title STRING,
    text_embeddings ARRAY<FLOAT> NOT NULL,
    image_embeddings ARRAY<DOUBLE> NOT NULL
) USING lance
TBLPROPERTIES (
    'text_embeddings.arrow.fixed-size-list.size' = '384',
    'image_embeddings.arrow.fixed-size-list.size' = '512'
);
```

### Working with Vector Tables

Once created, you can insert data using SQL:

```sql
-- Insert vector data (example with small vectors for clarity)
INSERT INTO embeddings_table VALUES
    (1, 'first text', array(0.1, 0.2, 0.3, ...)), -- 128 float values
    (2, 'second text', array(0.4, 0.5, 0.6, ...));
```

Query vector tables:

```sql
-- Select vectors
SELECT id, text FROM embeddings_table WHERE id = 1;

-- Count rows
SELECT COUNT(*) FROM embeddings_table;
```

### Vector Indexing

After creating and populating vector columns, you can create vector indexes using Lance Python API for similarity search:

```python
import lance

# Open the dataset
ds = lance.dataset("/path/to/embeddings_table.lance")

# Create a vector index on the embeddings column
ds.create_index(
    "embeddings",
    index_type="IVF_PQ",
    num_partitions=256,
    num_sub_vectors=16
)

# Perform similarity search
import numpy as np
query_vector = np.random.rand(128).astype(np.float32)
results = ds.to_table(
    nearest={"column": "embeddings", "q": query_vector, "k": 10}
).to_pandas()
```