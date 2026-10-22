# Operations

Integration tests cover these rows in both catalog modes; a few non-Delta details (managed-table locations, column
changes) are covered by unit tests.

| Operation | Delta tables | Other data source tables (Parquet, ORC, CSV, JSON, ...) |
|---|---|---|
| `SHOW TABLES`, table exists, `DESCRIBE`, load | Yes; loaded through Delta's `DeltaCatalog` | Yes |
| Reads | Yes, including time travel | Through Spark's file path, only for tables whose partitions are not tracked in Glue; time travel fails (Delta only) |
| `INSERT`, `INSERT OVERWRITE` | Yes | Files are written at the table location through Spark's file path; the Glue entry is not updated (no partitions, no statistics) |
| `MERGE`, `UPDATE`, `DELETE`, `OPTIMIZE`, `VACUUM`, `DESCRIBE HISTORY` by name | Yes | No (Delta only) |
| `CREATE TABLE ... LOCATION` | Yes: creates the Delta table there (or registers the one already there), then the Glue entry | Yes, Glue entry only |
| `CREATE TABLE` without `LOCATION` (managed) | Location from the database location, else `<warehouse>/<db>.db/<table>`; fails with neither | Same, Glue entry only |
| `CREATE TABLE ... AS SELECT`, `df.write.saveAsTable` | Yes, with Delta's semantics (default mode and `overwrite`, with or without the `path` option) | No: fails and points to Delta |
| `REPLACE TABLE`, `CREATE OR REPLACE TABLE`, `REPLACE TABLE ... AS SELECT` | Yes | No |
| `df.writeTo(...)`: `create`, `append`, `overwrite`, `overwritePartitions`, `replace`, `createOrReplace` | Yes; `append` and `overwrite` leave the Glue entry alone | `create`, `replace`, `createOrReplace`: no; `append`, `overwrite`, `overwritePartitions`: not tested |
| Structured Streaming: `writeStream.toTable`, `readStream.table` | Yes; `toTable` creates the table and its Glue entry when missing | Not tested |
| `ALTER TABLE` | Applied by Delta to its log. Column changes (add, drop, rename, type, comment, nullability, position; Delta's own rules apply) also rewrite the Glue columns. `SET LOCATION` needs a Delta table at the target, moves the Glue entry and re-syncs its columns. Properties stay in the Delta log | Properties, comment, top-level columns and location, in the Glue entry |
| `REFRESH TABLE` (SQL) | Re-syncs the Glue columns from the Delta log; needs Glue write access | No-op |
| `ALTER TABLE ... RENAME TO` | Glue entry only; the data stays in place | Same |
| `DROP TABLE` | Removes the Glue entry, never deletes data | Same |
| `DROP TABLE ... PURGE` | Fails | Fails |
| Views, functions | `CREATE VIEW` in a named catalog fails with Spark's `UNSUPPORTED_FEATURE.CATALOG_OPERATION` ("does not support views"); as `spark_catalog`, Spark sends them to its in-memory catalog, which has only `default`, so `CREATE VIEW` in a Glue database fails with `SCHEMA_NOT_FOUND`; temporary views work in both modes | Same |

Path-based Delta operations (`` delta.`s3://...` ``) involve no catalog and work as before.

Databases: `CREATE DATABASE` (comment, location, properties; without a location and with `warehouse` set, the database
is placed at `<warehouse>/<db>.db`, because Spark's Hive path cannot create tables in a database without one),
`ALTER DATABASE`, `DROP DATABASE` (with `CASCADE`, Glue's `DeleteDatabase` removes the database's table entries; table
data is never deleted), `SHOW DATABASES` and `USE`. Glue has single-level namespaces only.
Database and table names are lowercased (Glue stores them in lowercase), so `acct_a.MyDb.MyTable` and
`acct_a.mydb.mytable` are the same table; column names keep their case.

Iceberg tables in Glue fail on load with an error that points to Iceberg's `GlueCatalog`. Glue views are not listed
and cannot be loaded. Hive SerDe tables without `spark.sql.sources.provider` (written by Athena, Glue crawlers or
Trino's Hive connector) fail on load with a clear error.

## REFRESH TABLE and column sync

The Glue columns of a Delta table are written at create, replace, `ALTER TABLE` column changes and SQL
`REFRESH TABLE`. Schema evolution inside writes (`mergeSchema`, `MERGE ... WITH SCHEMA EVOLUTION`) does not update
them: run SQL `REFRESH TABLE` after those, which needs Glue write access. `spark.catalog.refreshTable` does not
re-sync the columns. See [How it works](how-it-works.md#column-sync).
