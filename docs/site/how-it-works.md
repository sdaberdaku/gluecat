# How it works

## Per-catalog Hadoop configuration {#per-catalog-hadoop-configuration}

The `hadoop.*` options and the derived S3 defaults (see [Configuration](configuration.md#per-table-hadoop-options))
have to reach the Hadoop configuration that Delta and Spark use for one table, while a session shares one Hadoop
configuration between all catalogs. They travel as table storage properties, the mechanism both already honour.

- Loads: the `CatalogTable` that gluecat hands out carries the options in `storage.properties` (the table's own
  properties win). Delta copies the `fs.*` and `dfs.*` keys from there into the Hadoop configuration of its
  `DeltaLog` (`DeltaTableV2.deltaLog`). For other data sources, Spark applies every storage property to the Hadoop
  configuration of the read or write (`newHadoopConfWithOptions`). Reads, inserts, time travel, `REFRESH` and
  non-Delta tables are covered this way.
- Staged writes (CTAS, `REPLACE ... AS SELECT`, `CREATE OR REPLACE ... AS SELECT`, `writeTo`): the options are added
  to the write options of the staged table's `WriteBuilder`, and Delta builds its Hadoop configuration from them. The
  caller's own options win, including those from SQL `OPTIONS (...)`. While Delta's
  `spark.databricks.delta.legacy.storeOptionsAsProperties` is `true`, Delta would persist every write option in the
  table metadata, so gluecat adds none and logs one warning per catalog.
- Everything else that opens the table's location (a plain `CREATE`, `REPLACE` without a query, the load by path after
  a create, `SET LOCATION`): gluecat opens the location's `FileSystem` with the catalog's configuration first.

Nothing is written into the Glue entry or the Delta log. The options are never table properties (`option.<key>`),
because Delta would store those in the log.

### The FileSystem cache

Hadoop caches `FileSystem` instances by scheme, bucket and user, not by configuration. The first access to a bucket in
a JVM fixes its credentials and endpoint. gluecat's own calls open the location with the catalog's configuration so
that the cache holds the right instance before Delta or Spark ask with the session configuration, and it relies on the
cache being enabled. It does not disable the cache: a new, never-closed `S3AFileSystem` per configuration on every
executor task would leak. The operator's rule is one bucket, one way: through one catalog's role or through the
session configuration, never both. Direct path access (`` delta.`s3://...` ``, `spark.read.parquet`) never reaches a
catalog and uses whatever the cache already holds for that bucket, else the session configuration.

## Why Delta's create path cannot be used as is

Delta's create path (`CreateDeltaTableCommand`) registers a new or replaced table through the catalog that
`DeltaCatalog` wraps (its delegate) only when that catalog's class name starts with `io.unitycatalog.`
(`AbstractDeltaCatalog.isUnityCatalog`, checked in Delta 4.4.1). For any other catalog it registers the table through
`spark.sessionState.catalog`, the session's built-in catalog behind `spark_catalog`. That catalog is not pluggable
(`spark.sql.catalogImplementation` accepts only `in-memory` and `hive`), and a session has exactly one. So gluecat
never passes a catalog identifier to Delta's create calls (`createTable`, `stageCreate`, `stageReplace`,
`stageCreateOrReplace`).

## Delta creates and replaces: run by path, then write Glue

For Delta tables, every create, replace and CTAS runs on the path identifier `` delta.`<location>` ``, which Delta
handles without any catalog, and gluecat then writes or updates the Glue entry itself.

- `CREATE TABLE` without a query, and `writeStream.toTable` on a missing table (Spark creates the table with a plain
  create, then streams into the loaded table): gluecat resolves the location (explicit, or derived for managed tables from the
  database location or `warehouse`), calls `DeltaCatalog.createTable` with the path identifier, creates the Glue
  entry, and returns the table loaded by name. If a Delta table already exists at the location, Delta's own rules
  apply: no columns given registers it as it is; columns given must match.
- CTAS, RTAS, `saveAsTable` and `writeTo` `create`, `replace` and `createOrReplace`: gluecat returns a staged table that wraps Delta's
  staged table for the path identifier, and Spark writes the query result into it. On commit, Delta commits by path
  first (data and log), then gluecat creates or updates the Glue entry. An abort aborts Delta's stage and leaves Glue
  untouched. A CTAS onto a location that already holds a Delta table fails the way Delta's path CTAS fails.
- Path identifiers need `spark.sql.runSQLOnFiles = true` (the default). With it off, creates and replaces fail
  before Delta writes anything.

### When the Glue write fails after the Delta commit

If the Glue write fails after Delta committed (after the SDK's own retries), the Delta table is valid at its
location but has no Glue entry. The catalog throws `DeltaCommittedButGlueFailedException`, whose message names the
table, the location and the Glue error, and states that the Delta commit succeeded.

Running the CTAS again fails, because the location now holds a Delta table. The recovery is:

```sql
CREATE TABLE <table> USING delta LOCATION '<location>'
```

This is idempotent: it registers the existing table, and fails only if an entry already exists.

For a replace, Delta's commit is a single atomic log entry. If the Glue update then fails, the Glue entry keeps its
previous columns, which no reader uses for Delta tables, and `REFRESH TABLE` re-syncs them. Delta's own history
(`RESTORE`) covers rolling back the data.

## The UpdateCatalog hook {#updatecatalog-hook}

Delta's `UpdateCatalog` post-commit hook, when enabled (`spark.databricks.delta.catalog.update.enabled`, default
`false`), updates the table through `spark.sessionState.catalog`, which qualifies the name to `spark_catalog`. With a
same-named table there, it would update that table instead of the Glue table.

That is why the catalog refuses to run while the hook is on. `initialize` fails while the setting is `true`. It is a
runtime setting, so the catalog checks it again before every Delta operation by name. While it is on, loads, reads,
writes, creates, alters and refreshes of tables by name fail; `DROP TABLE`, listings and database operations do not
go through the check.

## Why gluecat does not name a class `io.unitycatalog.*`

Naming a class `io.unitycatalog.*` would open Delta's catalog-plugin create path, but in Delta 4.4.1 that flag also
turns on Unity Catalog-specific behaviour: catalog-managed (catalog-owned commit) tables for managed creates,
Unity Catalog's managed-location rules, routing creates to a Unity Catalog REST client when the options enable one,
blocking the variant feature on Spark 4.0, and Unity Catalog table-ID property handling. Each new Delta release can
add more behind the same flag. So gluecat does not do it.

The long-term fix is upstream: a Delta change that lets any catalog opt into that create path (the `TODO` next to
the check says the restriction exists because of a `V2SessionCatalog` bug). Once a Delta release has it, gluecat can
use it there and keep the path-based route for older releases.

## Alter, rename, drop

- Delta tables: `ALTER TABLE` goes to `DeltaCatalog.alterTable`, and Delta applies the change to its log. Column
  changes (add, drop, rename, type, comment, nullability, position) also re-sync the Glue columns afterwards.
  Table properties of Delta tables stay in the Delta log. See [Column sync](#column-sync).
- Other providers: gluecat applies the change to the Glue entry.
- Every Glue `UpdateTable` passes the `VersionId` read before the change, so a concurrent change makes the update
  fail with a retryable error instead of overwriting it. `SkipArchive` follows `glue.skip-archive`.
- Glue has no rename. `RENAME` creates the new entry from the old one, then deletes the old one; the data does not
  move. It is not atomic: if the delete fails, both entries exist and point at the same location, and dropping the
  old name (which never deletes data) finishes the rename.

## Glue entry format rationale

The entries start from the shape Spark's `HiveExternalCatalog` writes for data source tables, because that is what
Trino and Spark's Hive-client path already read. Two additions serve Delta:

- the real `StorageDescriptor.Location` instead of Spark's `-__PLACEHOLDER__` suffix, and the parameter
  `table_type = DELTA`, which is what Athena's Delta support needs;
- the SerDe parameter `path` carrying the real location, which Trino's Delta connector and Spark's Hive path read
  first, so the real `StorageDescriptor.Location` is safe. The placeholder exists so that the Hive metastore does not
  manage a directory at the table location; Glue never touches the location.

Tables are marked `EXTERNAL_TABLE` even when managed, because the catalog never deletes data. Details:
[Glue entry format](glue-entry-format.md).

## Column sync {#column-sync}

Spark and Trino read the Delta schema from `_delta_log`, so gluecat does not keep the Glue columns in sync with
every commit. It writes them at create, replace, `ALTER TABLE` column changes and SQL `REFRESH TABLE`, which re-syncs
them from the Delta log (and needs Glue write access).

Schema evolution inside writes (`mergeSchema`, `MERGE ... WITH SCHEMA EVOLUTION`) does not trigger a sync: run SQL
`REFRESH TABLE` after those. `spark.catalog.refreshTable` does not re-sync the columns. `ALTER TABLE ... SET
LOCATION` on a Delta table needs a Delta table at the target, moves the Glue entry and re-syncs its columns.

## Errors

Glue and AWS errors map to Spark exceptions in one place, used by every code path:

| Glue / AWS error | Spark exception |
|---|---|
| `EntityNotFoundException` on a table call | `NoSuchTableException` |
| `EntityNotFoundException` on a database call | `NoSuchNamespaceException` |
| `AlreadyExistsException` on a table call | `TableAlreadyExistsException` |
| `AlreadyExistsException` on a database call | `NamespaceAlreadyExistsException` |
| `ConcurrentModificationException` (stale `VersionId`) | a retryable exception of this library |
| `AccessDeniedException`, STS errors, other AWS errors | the AWS exception, wrapped, with the catalog name, the `glue.id` and the role ARN when there is one |
| Glue write failed after a Delta commit | `DeltaCommittedButGlueFailedException` |
