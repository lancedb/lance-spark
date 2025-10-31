Lance supports traditional schema evolution: adding, removing, and altering columns in a dataset. Most of these operations can be performed without rewriting the data files in the dataset, making them very efficient operations. In addition, Lance supports data evolution, which allows you to also backfill existing rows with the new column data without rewriting the data files in the dataset, making it highly suitable for use cases like ML feature engineering.

We extend Spark's SQL syntax to support the `ALTER TABLE <table_name> ADD COLUMNS <column_a, colum_b, ...> FROM <temporary_view>` statement, which allows you to add new columns to a dataset and backfill them with data. The <temporary_view> is a temporary view identifier which can be defined using normal SELECT statement.

To use this feature, you need to set 

`--conf spark.sql.extensions=com.lancedb.lance.spark.extensions.LanceSparkSessionExtensions` 

for your Spark application.

Because we use `_rowaddr` and `_fragid` to address the target dataset's rows for the new column's data. So, the temporary view should contain `_rowaddr` and `_fragid`.

Example:

```sql
CREATE TEMPORARY VIEW tmp_view
AS
SELECT _rowaddr, _fragid, hash(name) as name_hash 
FROM users;

ALTER TABLE users ADD COLUMNS name_hash FROM tmp_view;
```

No table rewrite, no data movement—just a new column that is instantly queryable.
