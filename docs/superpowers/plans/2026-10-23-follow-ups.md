# Follow-ups after 1.0.0

From the whole-branch review of 2026-10-23. None blocks 1.0.0; each is small.

## 1.0.1 candidates

- Share one Glue client (and its credentials provider) per distinct catalog options across Spark sessions. Spark
  never closes catalog plugins and creates new ones per session; today each session keeps its own `GlueClient`
  and, with assume role, its own `StsClient`, both registered in the SDK's idle-connection reaper until the JVM exits.
  `SdkGlueApi.close()` should also close the STS provider and client and the shared HTTP client.
- `spark.catalog.listTables(db)` on Spark 4.x throws `UnsupportedTableException` when the database also holds Iceberg,
  Hive SerDe or malformed entries (Spark loads every listed table). SQL `SHOW TABLES` is unaffected.
- Check at `initialize` that the artifact's Spark minor matches the running Spark (`BuildInfo`), so a wrong artifact
  fails with a clear message instead of a later `NoSuchMethodError`.
- Compile each artifact against the floor patch of its Spark line (3.5.3, 4.0.0, 4.1.1, 4.2.0) and test forward,
  instead of compiling against the newest patch and testing backward.
- Wrap `NoClassDefFoundError` and `LinkageError` from `initialize` with an actionable message ("add
  software.amazon.awssdk:glue and sts to the classpath").
- Write Glue `PartitionKeys` in partition-column order at create time (today: schema order; the first ALTER rewrites
  them in partition-column order).
- Quote identifiers and escape the location in the `DeltaCommittedButGlueFailedException` recovery statement; make
  the nested concurrent-modification text consistent with the outer REFRESH advice.
- Option parsing: case-insensitive keys with a warning on case-mismatched known keys; invalid boolean names the key;
  redact `credentialsProviderOptions` and `assumeRoleExternalId` in `GlueCatalogOptions.toString`, and
  `GlueCatalogOptions.hadoop` and `TableHadoopOptions.values`.
- `table_type` in CREATE properties rejected like on ALTER; a
  bare `option.` key rejected; nicer message for `RENAME TO` with another catalog name.
- `dropNamespace` emptiness check should stop at the first table instead of paging through all of them.
- Maven wrapper `distributionSha256Sum`; `META-INF/*.EC` in the packaging test; `compat-down` should `chmod 0777`
  the shared directory it recreates.
- Test robustness: compat suites are order-dependent within a suite; a few `intercept[Exception]` could pin the
  reason; `PublishedPomSuite` should use `basedir` so it also runs from an IDE.

## 1.1.1 candidates

From the 1.1.0 whole-branch review of 2026-10-24.

- Pass the catalog's Hadoop options into Delta's by-path operations; a suite with `fs.s3a.impl.disable.cache=true`
  covering loads, INSERT and CTAS.
- SET LOCATION and `syncSchema` build the new entry from an earlier read but send the VersionId of a second read, so a
  concurrent change between the two reads is overwritten. Add `replaceEntry(ident, basedOn, table)` sending
  `basedOn.versionId`, like `alterEntry`, with an InMemoryGlueApi test.
- `PassThroughFormatSupport` must refuse `USING delta` creates and alters of Delta entries when Delta is not on the
  classpath, with a unit test over InMemoryGlueApi.

## Later

- One exception family for Delta-committed partial failures (replace, alter+sync, rename).
- A local-session `DeltaFormatSupportSuite` and unit tests for `prime`.
- `HadoopOptionsIT` tests should not depend on each other's state.
- The unused `src/main/spark-${spark.minor}/scala` source hook: use it for a Spark 4.1+ `listTableSummaries`
  override, or remove it and amend the spec.
- Build once and test the published jars.
- Shared module pom dependencies if a sixth module comes.
- Mark internals `private[gluecat]` in 1.2.0 and add MiMa for `GlueCatalog` and the exceptions.
- A `CREATE TABLE ... SHALLOW CLONE` / `CONVERT TO DELTA` IT asserting nothing lands in `spark_catalog`.

## Proposed feature (not started)

- `glue.sync-schema-on-commit` (default off): a `QueryExecutionListener` that re-syncs the Glue columns of a Delta
  table after writes by name that changed its schema, the equivalent of Delta's
  `spark.databricks.delta.catalog.update.enabled` without the `spark_catalog` routing.

## Upstream

- Propose a Delta change that lets any wrapping catalog opt into `createTableFunc`, so creates no longer need the
  by-path route on releases that have it.
