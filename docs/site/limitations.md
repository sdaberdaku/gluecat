# Limitations

- One writer per Delta table: Delta-Spark's default S3 LogStore is safe for one driver only, until Delta gets a
  conditional-write LogStore.
- The Glue columns of a Delta table are written at create, replace, `ALTER TABLE` column changes and
  `REFRESH TABLE`, not after schema evolution inside writes (`mergeSchema`, `MERGE ... WITH SCHEMA EVOLUTION`): run
  SQL `REFRESH TABLE` after those. `REFRESH TABLE` on a Delta table needs Glue write access.
  `spark.catalog.refreshTable` does not re-sync the columns.
- Non-Delta tables: only tables whose partitions are not tracked in Glue are readable; `INSERT` writes files but does
  not update the Glue entry; `MERGE`, `UPDATE`, `DELETE`, CTAS and `REPLACE` are Delta only.
- Views and functions: named catalogs refuse `CREATE VIEW` with Spark's `CATALOG_OPERATION` error. As
  `spark_catalog`, persistent views and functions work only in the in-memory catalog's `default` database and live
  only as long as the application; in a Glue database `CREATE VIEW` fails with `SCHEMA_NOT_FOUND`. Temporary views
  work in both modes. Materialized views are not supported.
- Generated columns and column `DEFAULT` values are refused: Spark refuses generated columns because the catalog
  declares no support for them; Delta refuses `ADD COLUMN ... DEFAULT`, and the catalog refuses it for other tables,
  whose Glue entry has nowhere to keep the default.
- `DROP` never deletes data and `PURGE` is not supported. This differs from Spark's managed tables: dropped managed
  tables leave their directories under `<warehouse>/<db>.db/` or the database location, and repeated create and drop
  cycles accumulate data there that has to be cleaned up separately.
- `RENAME` is not atomic: Glue has no rename, so the catalog creates the new entry, then deletes the old one. If the
  delete fails, both entries exist and point at the same location; dropping the old name finishes the rename.
  Renaming a Delta table loads its Delta log first, so a table whose log is missing cannot be renamed.
- `ALTER TABLE ... SET LOCATION` on a Delta table requires a Delta table (a `_delta_log` directory) at the target.
- `REPLACE` cannot move a table: a `LOCATION` other than the current one fails (`s3`, `s3a` and `s3n` count as the
  same scheme, and a trailing slash does not matter).
- Table properties that the catalog keeps in Glue itself (`EXTERNAL`, `table_type`, `transient_lastDdlTime`,
  `spark.sql.*`) cannot be set or removed, and neither can a table-level `collation` (Spark 4.1 and later).
- Delta creates and replaces run on the table path, so they need `spark.sql.runSQLOnFiles = true` (the default); with
  it off they fail before Delta writes anything.
- Delta's `UpdateCatalog` hook would write through `spark_catalog` (see [How it works](how-it-works.md#updatecatalog-hook)), so the catalog fails to
  initialize while `spark.databricks.delta.catalog.update.enabled` is `true`. It is a runtime setting: the catalog
  checks it again before every Delta operation by name. While it is on, loads, reads, writes, creates, alters and
  refreshes of tables by name fail; `DROP TABLE`, listings and database operations do not go through the check.
- Spark's statistics auto-update (`spark.sql.statistics.size.autoUpdate.enabled`, default `false`) makes an `INSERT`
  into a non-Delta table read and alter the statistics of the same-named table in `spark_catalog`'s built-in catalog
  (`CommandUtils.updateTableStats` goes through `spark.sessionState.catalog`), or fail after writing when there is
  none. While it is `true`, loads, reads, writes and creates of non-Delta tables by name fail before anything is
  written. Delta tables are not affected.
- Concurrent table alters: every Glue `UpdateTable` passes the `VersionId` read before the change, so the second of
  two concurrent alters fails with a retryable error; run the statement again. Glue databases have no `VersionId`, so
  concurrent database alters are last-writer-wins.
- As `spark_catalog` on Spark 4.1 and later, path-based Delta DDL (`` CREATE TABLE delta.`<path>` ``,
  `` REPLACE TABLE delta.`<path>` ``, `ALTER TABLE ... ADD COLUMNS` on a path) makes Spark call `GetDatabase` for a
  database named `delta`, to look up a default collation. Spark ignores the result, but the call shows up in
  CloudTrail, possibly as access denied.
- Delta 4.1.0 and 4.2.0 fail every `ALTER TABLE` on a Delta table on Spark 4.1.2 and later
  (`NoClassDefFoundError: IgnoreCachedData`), with or without gluecat. Use Delta 4.3.1 or later there (the versions CI tests).
- On Spark 4.x, `spark.catalog.listTables` fails with the catalog's `UnsupportedTableException` when the database
  holds an entry the catalog cannot load (an Iceberg table, a Hive SerDe table without `spark.sql.sources.provider`):
  it loads every table it lists and tolerates only Spark's own error conditions. `SHOW TABLES` lists such entries,
  and so does `spark.catalog.listTables` on Spark 3.5.
- STS: see the notes under [Credentials](configuration.md#credentials) (regional public endpoint, `AWS_ENDPOINT_URL` ignored by SDK 2.24.6, private
  DNS for VPC endpoints).
- Hadoop's `FileSystem` cache is keyed by scheme, bucket and user, not by configuration: the first access to a bucket
  in a JVM fixes its credentials and endpoint. Reach each bucket one way only, through one catalog's role or through
  the session configuration, never both. gluecat relies on the cache being enabled (Hadoop's default). Direct path
  access (`` delta.`s3://...` ``, `spark.read.parquet`) always uses the session configuration. See
  [Configuration](configuration.md#one-bucket-one-way).
- `hadoop.*` options reach Delta only as `fs.*` and `dfs.*` keys; other Hadoop keys reach non-Delta data sources only.
  With `spark.databricks.delta.legacy.storeOptionsAsProperties` on, Delta writes get the options only through the
  cached `FileSystem`, not as write options.
- `fs.s3a.assumed.role.external.id` exists in Hadoop 3.4 (Spark 4.x) only; Spark 3.5's Hadoop 3.3 ignores it. On
  Spark 3.5 a catalog without a role needs `hadoop.fs.s3a.endpoint.region` (or the session's) to avoid a 301 from S3.
- Tested against MiniStack (a local AWS emulator) only. The suite runs under IAM enforcement and a role with exactly the published Glue policy, but
  the emulator's Glue resource ARNs are coarser than AWS's, so resource-scoped policies, Glue resource policies and cross-account
  `CatalogId` access are not exercised against real AWS. Cross-account behaviour is tested by request shape (which
  `CatalogId` and which STS identity each call uses), not by AWS authorization outcomes.
