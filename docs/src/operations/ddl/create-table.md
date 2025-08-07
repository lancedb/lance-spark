# CREATE TABLE

Create new Lance tables with SQL DDL statements.

Create a simple table:

```sql
-- 
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