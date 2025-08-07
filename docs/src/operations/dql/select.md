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

Join queries:

```sql
SELECT u.name, p.title
FROM users u
JOIN projects p ON u.id = p.user_id;
```