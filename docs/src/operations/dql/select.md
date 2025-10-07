# SELECT

Query data from Lance tables using standard SQL SELECT statements.

Select all data from a table:

```sql
SELECT * FROM users;
```

Select specific columns:

```sql
SELECT id, name, email FROM users;
```

Query with WHERE clause:

```sql
SELECT * FROM users WHERE age > 25;
```

Aggregate queries:

```sql
SELECT department, COUNT(*) as employee_count
FROM users
GROUP BY department;
```

### Count Star Optimization

Lance-Spark automatically optimizes `COUNT(*)` queries through aggregate pushdown. When you use `COUNT(*)`, the query scans only the `_rowid` metadata column instead of reading all data columns, which significantly improves performance especially for tables with large binary data or many columns.

```sql
-- Optimized count query
SELECT COUNT(*) FROM users;

-- Count with filter (also optimized)
SELECT COUNT(*) FROM users WHERE age > 25;

-- Group by count (optimized)
SELECT department, COUNT(*)
FROM users
GROUP BY department;
```

This optimization is automatic and requires no special configuration. For tables with blob columns or large datasets, `COUNT(*)` queries can be orders of magnitude faster than scanning all columns.

Join queries:

```sql
SELECT u.name, p.title
FROM users u
JOIN projects p ON u.id = p.user_id;
```

## Querying Blob Columns

When querying tables with blob columns, the blob data itself is not materialized by default. Instead, you can access blob metadata through virtual columns.

### Selecting Blob Metadata

Query blob position and size information:

```sql
SELECT id, title, content__blob_pos, content__blob_size
FROM documents
WHERE id = 1;
```

The virtual columns available for blob columns are:
- `<column_name>__blob_pos` - Byte position in the blob file
- `<column_name>__blob_size` - Size of the blob in bytes

### Filtering and Aggregating

You can filter and aggregate using blob metadata:

```sql
-- Find large blobs
SELECT id, title, content__blob_size
FROM documents
WHERE content__blob_size > 1000000;

-- Get blob statistics
SELECT
    COUNT(*) as blob_count,
    AVG(content__blob_size) as avg_size,
    MAX(content__blob_size) as max_size
FROM documents;
```

### Selecting Non-Blob Columns

When you don't need the blob data, select only the non-blob columns for better performance:

```sql
-- Select without blob column
SELECT id, title, created_at
FROM documents
WHERE title LIKE '%report%';
```

**Note**: The blob column itself returns empty byte arrays when selected. To access actual blob data, you would need to use the position and size information to read from the blob file using external tools or custom logic.