# INSERT INTO

Add data to existing Lance tables.

Insert individual rows:

```sql
INSERT INTO users VALUES 
    (4, 'David', 'david@example.com', '2024-01-15 10:30:00'),
    (5, 'Eva', 'eva@example.com', '2024-01-15 11:45:00');
```

Insert with column specification:

```sql
INSERT INTO users (id, name, email) VALUES 
    (6, 'Frank', 'frank@example.com'),
    (7, 'Grace', 'grace@example.com');
```

Insert from SELECT query:

```sql
INSERT INTO users
SELECT user_id as id, username as name, email_address as email, signup_date as created_at
FROM staging.user_signups
WHERE signup_date >= '2024-01-01';
```

Insert with complex data types:

```sql
INSERT INTO events VALUES (
    1001,
    123,
    'page_view',
    array('web', 'desktop'),
    struct('web_app', 1, '2024-01-15 12:00:00'),
    '2024-01-15 12:00:00'
);
```