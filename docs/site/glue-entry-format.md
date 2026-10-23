# Glue entry format

The entry format is public API from 1.0.0.

Entries follow the shape Spark's Hive path (`HiveExternalCatalog`) writes for data source tables, which Trino and
Spark's Hive-client path read, with two additions for Delta so that Athena's Delta support can read the tables too.

## What gluecat writes

- `TableType = EXTERNAL_TABLE` and parameter `EXTERNAL = TRUE`, for managed tables too: the catalog never deletes
  data, so Glue should not treat any table as managed.
- `spark.sql.sources.provider = <provider>`, `spark.sql.create.version = <Spark version>`,
  `spark.sql.partitionProvider = filesystem`.
- `spark.sql.sources.schema = <schema JSON>`, split into `spark.sql.sources.schema.numParts` and
  `spark.sql.sources.schema.part.N` when it exceeds Spark's length threshold
  (`spark.sql.sources.schemaStringLengthThreshold`, default 4000 characters; parts are numbered from 0). Partition
  columns go in `spark.sql.sources.schema.numPartCols` and `spark.sql.sources.schema.partCol.N`.
- The real location in the SerDe parameter `path`, which Trino's Delta connector and Spark's Hive path read first.
- Delta tables also carry `StorageDescriptor.Location` set to the real location (not Spark's `-__PLACEHOLDER__`
  suffix) and the parameter `table_type = DELTA`, which is what Athena's Delta support reads. SerDe, input and output
  formats are the ones Spark writes for non-Hive-compatible providers (`LazySimpleSerDe`,
  `SequenceFileInputFormat`, `HiveSequenceFileOutputFormat`).
- Other providers get what `HiveExternalCatalog.createDataSourceTable` would write, including its choice between the
  Hive-compatible form (real SerDe, input and output formats, columns and location, for providers such as Parquet and
  ORC) and the Spark-specific form with the placeholder location.
- `StorageDescriptor.Columns`: the table's columns (name and Spark `catalogString` type) at create, replace,
  `ALTER TABLE` column change or `REFRESH TABLE` time. No `PartitionKeys`.
- User table properties as Glue parameters.
- Glue column comments on one line (control characters become spaces) and cut to 255 UTF-8 bytes, and the table
  description cut to 2048 bytes, Glue's limits; the full column comments stay in the schema JSON, and a Delta
  table's comment in its log.
- `Retention` 0 on the table, and `Compressed` false, `NumberOfBuckets` -1 and `StoredAsSubDirectories` false in the
  `StorageDescriptor`, as Hive writes them; the Glue Hive client unboxes them on read. An `UpdateTable` rewrites
  them, so bucketed legacy entries lose their bucket count, consistent with the model dropping bucket columns.

`classification` is not set: Athena's Delta documentation does not use it, and it is a Glue crawler and ETL
convention.

## Rules

- Required for loading: `spark.sql.sources.provider` and a location (the SerDe `path`, else
  `StorageDescriptor.Location` without the placeholder suffix). Everything else is optional when reading; a Delta
  entry with no schema loads, because Delta reads the schema from its log. The provider check is case-insensitive.
- Unknown parameters (written by other tools, for example `trino_*` or `delta.*` keys) are preserved on every update:
  an `UpdateTable` starts from the entry as read and changes only the keys the operation owns.
- A missing schema part is a load error that names the table. Malformed entries (bad schema JSON, missing location,
  a part count that does not match, a partition column that is not in a non-Delta table's schema) fail on load with an
  error that names the table and the problem.
- Non-Delta tables load with their partition columns last, in `partCol` order, as `HiveExternalCatalog` restores
  them (Spark reads and writes them as data columns followed by partition columns). Delta tables keep the stored
  order.
- Names are lowercase.

## Read variants

The catalog loads, in addition to its own entries:

- an entry written by Spark's Hive path (3.5 and 4.1) for a Delta CTAS: columns `col array<string>`,
  `spark.sql.sources.schema = {"type":"struct","fields":[]}`, `spark.sql.partitionProvider = catalog`;
- an entry whose columns and properties Delta rewrote after a `REPLACE` or overwrite: real columns plus
  `delta.lastUpdateVersion`, `delta.lastCommitTimestamp`, `delta.minReaderVersion`, `delta.minWriterVersion`;
- schemas stored in parts;
- entries with a placeholder location or a real `StorageDescriptor.Location`, the SerDe `path` taking precedence;
- non-Delta data source tables written by Spark's Hive path, in both of its forms.

Iceberg tables in Glue fail on load with an error that points to Iceberg's `GlueCatalog`. Glue views are not listed
and cannot be loaded. Hive SerDe tables without `spark.sql.sources.provider` (written by Athena, Glue crawlers or
Trino's Hive connector) fail on load with a clear error.

## Reader compatibility

Checked by the compatibility suites (`TrinoCompat*IT`, `LegacyHiveGlueCompat*IT`, against MiniStack in both catalog
modes; see [Development](development.md)):

- **Trino 484** reads gluecat Delta tables by name (rows, `SHOW TABLES`, `DESCRIBE`, rows added later, a partitioned
  CTAS), and gluecat reads a Delta table Trino created. In the tests Trino uses its native local file system, so
  Trino cannot `INSERT` there; Trino's own Delta entry has `spark.sql.sources.provider = DELTA` (upper case).
- **Spark 4.1 with the Glue Hive client** (legacy; Spark 4.1.3, Delta 4.4.1) and gluecat read each other's tables in
  both directions. The legacy client creates a Delta CTAS entry of the same shape as the one in the read variants
  above. The legacy Hive path puts a table's placeholder under its database location, so it cannot create Delta
  tables in a database that has no location; `CREATE DATABASE` without `LOCATION` in gluecat (and no `warehouse`)
  writes none.
- **Athena** is the reason for the `table_type = DELTA` parameter and the real `StorageDescriptor.Location`; it is not
  exercised by the tests.
