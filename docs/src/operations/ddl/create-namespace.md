# CREATE NAMESPACE

Create new child namespaces in the current Lance namespace.

Create a simple namespace:

```sql
CREATE NAMESPACE company;
```

Create nested namespace:

```sql
CREATE NAMESPACE company.analytics;
```

Create namespace if it doesn't exist:

```sql
CREATE NAMESPACE IF NOT EXISTS company;
```

Create namespace with properties:

```sql
CREATE NAMESPACE company WITH PROPERTIES (
    'description' = 'Company data namespace',
    'owner' = 'data-team'
);
```