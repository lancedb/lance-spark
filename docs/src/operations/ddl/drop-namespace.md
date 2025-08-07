# DROP NAMESPACE

Remove a child namespace from the current Lance namespace.

Drop an empty namespace:

```sql
DROP NAMESPACE company;
```

Drop namespace if it exists (no error if it doesn't exist):

```sql
DROP NAMESPACE IF EXISTS company;
```

Drop namespace and all its contents (CASCADE):

```sql
DROP NAMESPACE company CASCADE;
```

Drop nested namespace:

```sql
DROP NAMESPACE company.analytics;
```