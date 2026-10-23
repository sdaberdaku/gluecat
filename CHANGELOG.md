# Changelog

## 1.1.0 - unreleased

- Per-catalog Hadoop options: `spark.sql.catalog.<name>.hadoop.<key>` applies to every table the catalog loads,
  creates or replaces. Nothing is written to Glue or the Delta log. Only `fs.*` and `dfs.*` keys reach Delta.
- S3A inherits the catalog's role by default. A catalog with `client.assume-role.arn` (and no
  `client.credentials-provider`) now makes S3A assume the same role for its tables' files. This changes behaviour for
  existing catalogs with a role: set `hadoop.fs.s3a.aws.credentials.provider` to opt out. Session per-bucket keys
  (`fs.s3a.bucket.<name>.*`) still win. S3A's own STS call keeps the credentials chain the session used before:
  gluecat derives `fs.s3a.assumed.role.credentials.provider` from the session's effective
  `fs.s3a.aws.credentials.provider` (name the web-identity provider for IRSA). gluecat does not derive
  `fs.s3a.endpoint.region`. On Spark 3.5 (Hadoop 3.3), which cannot send an external ID, a catalog with
  `client.assume-role.external-id` derives nothing for S3A and logs a warning.
- `http-client.*` options (client type, timeouts, pool size, proxy) for the Glue and STS clients, with Iceberg's names.
  `software.amazon.awssdk:apache-client` and `url-connection-client` are optional `provided` dependencies; a missing
  one fails at `initialize` with a message naming the artifact and the missing class. The proxy keys apply to both
  client types.
- Every integration suite's main catalog runs under IAM enforcement, with a role that has exactly ten Glue actions.
  That policy is published in the documentation.
- New documentation page, "Across AWS accounts": identity model, IAM pieces, regions, sessions, S3, a worked example
  and troubleshooting.
- Known limitations of the per-catalog Hadoop options:
  - `writeStream.toTable` on a Delta table by name (and, on Delta 3.3, `readStream.table`) does not receive them:
    Spark starts the sink with the provider and location only and Delta builds its log without storage properties,
    so the session configuration is used. With a role-bearing catalog, streaming then reaches the bucket with the
    session identity while batch work uses the role. Workarounds: per-bucket session keys (`fs.s3a.bucket.<name>.*`)
    for the buckets that streaming writes, or `foreachBatch` with batch writes by name.
  - Priming the `FileSystem` and skipping the write options under
    `spark.databricks.delta.legacy.storeOptionsAsProperties` act in the driver JVM only. Executor-side Delta work for
    by-path operations (snapshot reconstruction when the latest log version has no `.crc` file, common for tables
    written by Trino or delta-rs; data files written under the legacy flag) uses the session configuration in any
    multi-JVM deployment.

### Upgrading from 1.0.0

- Every catalog with `client.assume-role.arn` (and no `client.credentials-provider`) now reaches S3 through that
  role, so the one-bucket-one-way rule applies to its buckets: anything else in the same JVM that reaches them with
  another identity (streaming checkpoints, direct path reads, other catalogs, streaming writes) shares whichever
  `FileSystem` opened the bucket first.
- `DESCRIBE EXTENDED`, `SHOW TBLPROPERTIES` and `SHOW CREATE TABLE` show the catalog's `option.fs.s3a.*` keys for
  non-Delta tables and for Delta before 4.3. Running a `SHOW CREATE TABLE` output again persists those keys into the
  Glue entry (non-Delta) or the Delta log (Delta), where they then override the catalog's values; remove them from
  the statement first.
- To keep 1.0.0's S3 behaviour for a catalog, set `hadoop.fs.s3a.aws.credentials.provider` to the session's provider
  list (the session's `fs.s3a.aws.credentials.provider`, or Hadoop's default list when the session sets none).
- `http-client.*` and `hadoop.*` keys, which 1.0.0 ignored silently, now take effect, and
  `http-client.type = urlconnection` needs `software.amazon.awssdk:url-connection-client` on the classpath. Keys for
  both client types (`http-client.apache.*` and `http-client.urlconnection.*`) without `http-client.type` now fail at
  `initialize`.

## 1.0.0 - 2026-10-23

First release.

- Artifacts `io.github.sdaberdaku:gluecat-spark-<minor>_<scala>` for Spark 3.5 (Scala 2.12 and 2.13, Java 8
  bytecode, Spark 3.5.3 and later) and Spark 4.0, 4.1 and 4.2 (Scala 2.13, Java 17 bytecode).
- `io.github.sdaberdaku.gluecat.GlueCatalog`: one Glue Data Catalog per Spark catalog, with its own catalog ID
  (`glue.id`) and credentials (custom provider class, STS assume role, or the SDK default chain including IRSA). Works
  as a named catalog and as `spark_catalog`.
- Delta tables: load, read, write and time travel by name through Delta's `DeltaCatalog`; `CREATE TABLE`, CTAS,
  `REPLACE`, `saveAsTable`, `writeTo` and `writeStream.toTable` run on the table path, then write the Glue entry.
  `ALTER TABLE` column changes and SQL `REFRESH TABLE` re-sync the Glue columns.
- Other data source tables (Parquet, ORC, CSV, JSON, ...): load and read, create and alter the Glue entry.
- Glue databases as Spark namespaces: create, alter, drop (with `CASCADE`), list, `USE`.
- `RENAME` and `DROP` change the Glue entry only; data is never deleted and `PURGE` is refused.
- Every Glue `UpdateTable` passes the `VersionId` it read; a concurrent change fails with a retryable error.
- The catalog refuses to run while Delta's `UpdateCatalog` hook is enabled, because the hook writes through
  `spark_catalog`.
- Non-Delta tables are refused by name while `spark.sql.statistics.size.autoUpdate.enabled` is on, because Spark's
  V1 insert path then updates statistics through `spark_catalog`.
- Non-Delta tables: partition columns are moved to the end of the schema on load, as Spark's Hive path does; dropping
  or renaming a partition column and `ADD COLUMN ... DEFAULT` are refused.
- Glue column comments are written single-line and cut to 255 bytes, table descriptions to 2048 bytes (Glue's
  limits); the schema JSON and the Delta log keep the full text.
- AWS SDK for Java v2 (`glue`, `sts`) as a `provided` dependency, built and tested against 2.24.6. Assume-role
  sessions refresh on the calling thread: Spark never closes catalog plugins, so no background refresh is started.
