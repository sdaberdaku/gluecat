# gluecat: design

gluecat is an AWS Glue Data Catalog client for Apache Spark.

Date: 2026-10-22. Status: approved 2026-10-22.

## Why

Spark has one built-in catalog, `spark_catalog`. Out of the box, Delta Lake works only through it
(`DeltaCatalog` wraps the session catalog), and the session catalog sits on a single Hive metastore
client. With the AWS Glue Hive client, that means one Glue catalog ID and one set of credentials for
the whole Spark application.

In a setup where each customer, team or environment has its own AWS account, the S3 data can already
live in each account (S3A handles per-bucket access), but every Delta table has to be registered in one
shared Glue catalog. A single Spark application, for example a Spark Connect server that serves jobs
for many accounts, cannot register account A's tables in account A's Glue and account B's tables in
account B's Glue.

Iceberg does not have this problem: each named Iceberg catalog gets its own Glue catalog ID and its own
IAM role (`glue.id`, `client.assume-role.arn`). This project brings the same model to Glue tables in
general and to Delta tables in particular, as a plain Spark catalog plugin: no Hive metastore client, no
patched Hive jars, AWS SDK for Java v2.

### Prior art (checked 2026-10-22)

- awslabs `aws-glue-data-catalog-client-for-apache-hive-metastore`: a Hive metastore client on SDK v1;
  open source Spark needs a patched Hive (HIVE-12679). Its `branch-3.5.0` (May 2026) moves to SDK v2 and
  adds a `GlueCatalogPlugin`, but the plugin only lists databases and tables (`loadTable` always
  throws), has no credentials configuration, and the branch builds only against EMR's Hive.
- `sdaberdaku/aws-glue-data-catalog-spark-client`: builds the patched Hive and the awslabs client per
  Spark release. Single catalog, SDK v1.
- Iceberg `GlueCatalog`: the configuration model we copy, but Iceberg tables only.
- Apache Gravitino Spark connector: federates several Glue catalogs but needs a Gravitino server, sends
  Hive-format tables through the patched Hive client, has static keys only and targets Spark 3.x.
- Kyuubi Spark Hive connector: several Hive metastores as named catalogs, through Spark's Hive client
  (same patch and SDK v1 problem).
- delta-io/delta#1679 (native Glue catalog for Delta): closed as stale in August 2026, never merged.
- Unity Catalog's Spark connector (`UCSingleCatalog`): a named catalog that wraps `DeltaCatalog`. We
  follow this pattern for loading Delta tables.

## Goals for 1.0.0

1.0.0 is done when the full CI matrix (below) passes and 1.0.0 is published on Maven Central, one
artifact per supported Spark minor and Scala version, with the README and the documentation site documenting
configuration and the known limitations:

| Artifact | Spark | Scala | Java bytecode | JDKs tested |
|---|---|---|---|---|
| `io.github.sdaberdaku:gluecat-spark-3.5_2.12` | 3.5.3+ | 2.12 | 8 | 8, 11, 17 |
| `io.github.sdaberdaku:gluecat-spark-3.5_2.13` | 3.5.3+ | 2.13 | 8 | 8, 11, 17 |
| `io.github.sdaberdaku:gluecat-spark-4.0_2.13` | 4.0.x | 2.13 | 17 | 17, 21 |
| `io.github.sdaberdaku:gluecat-spark-4.1_2.13` | 4.1.x | 2.13 | 17 | 17, 21 |
| `io.github.sdaberdaku:gluecat-spark-4.2_2.13` | 4.2.x | 2.13 | 17 | 17, 21, 25 |

Java and Scala support follow each Spark line's `docs/index.md`: 3.5 runs on Java 8/11/17 with Scala
2.12/2.13, 4.0 and 4.1 on Java 17/21 with Scala 2.13, 4.2 on Java 17/21/25 with Scala 2.13. Artifact
names include the Spark minor even where two jars would be identical, so a later split never renames
anything (artifact names are public API from 1.0.0). A new Spark minor gets a new artifact.

### Configuration

One named Spark catalog per Glue catalog:

```
spark.sql.catalog.acct_a                                 = io.github.sdaberdaku.gluecat.GlueCatalog
spark.sql.catalog.acct_a.glue.id                         = 111111111111
spark.sql.catalog.acct_a.client.region                   = eu-central-1
spark.sql.catalog.acct_a.client.assume-role.arn          = arn:aws:iam::111111111111:role/glue-access
spark.sql.catalog.acct_a.client.assume-role.external-id  = ...   # optional
spark.sql.catalog.acct_a.client.assume-role.session-name = ...   # optional
spark.sql.catalog.acct_a.warehouse                       = s3://bucket-a/warehouse   # optional
spark.sql.catalog.acct_b.glue.id                         = 222222222222
...
```

Property names follow Iceberg's AWS properties (`glue.id`, `glue.endpoint`, `glue.skip-archive`,
`client.region`, `client.assume-role.arn`, `.external-id`, `.session-name`, `.region`, `.timeout-sec`,
`client.credentials-provider` plus its `client.credentials-provider.*` options, `warehouse`). Check the
exact names and defaults against Iceberg's `AwsProperties` / `AwsClientProperties` and keep the ones
that apply.

Credentials, in order of precedence:

- `client.credentials-provider`: a user-supplied SDK v2 `AwsCredentialsProvider` class.
- `client.assume-role.arn`: an auto-refreshing STS assume-role provider whose base credentials come from
  the default chain.
- Otherwise the SDK default chain, including IRSA web identity on EKS.

A `glue.id` different from the caller's account without a role also works if the target account's Glue
resource policy allows it, because every Glue call passes `CatalogId`.

A catalog never calls any Glue catalog other than its own `glue.id`.

### Using it as `spark_catalog`

`GlueCatalog` can also replace the built-in session catalog, so unqualified `db.table` names and tools
that assume `spark_catalog` go to Glue without the Hive client:

```
spark.sql.catalogImplementation      = in-memory
spark.sql.catalog.spark_catalog      = io.github.sdaberdaku.gluecat.GlueCatalog
spark.sql.catalog.spark_catalog.glue.id = 111111111111
...
spark.sql.catalog.acct_b             = io.github.sdaberdaku.gluecat.GlueCatalog   # more catalogs as usual
```

This works because Spark only sends statements down its built-in V1 command path for the session
catalog when the session catalog is the built-in implementation or a `CatalogExtension`
(`ResolveSessionCatalog.supportsV1Command`, identical in 3.5.9, 4.0.1 and 4.2.0), and
`CatalogManager.loadV2SessionCatalog` wires a delegate only into a `CatalogExtension` and returns any
other plugin unchanged (`case other => other`, checked in 3.5.3, 3.5.9, 4.0.1, 4.1.2 and 4.2.0; there is
no "must implement CatalogExtension" check). `GlueCatalog` must therefore never implement
`CatalogExtension`. Spark's in-memory catalog stays in place only for temporary views and functions.
In this mode Delta's `UpdateCatalog` hook and Delta's `SessionCatalog` fallbacks hit the empty in-memory
catalog, not Glue.

Both modes are first-class: every integration test runs once with the catalog as a named catalog and
once as `spark_catalog`.

### Default namespace

Spark resolves unqualified table names in the current namespace, which starts as `default`. Glue has no
`default` database unless someone creates one (the awslabs Hive client creates it at startup when it is
missing). This catalog never writes on startup: `defaultNamespace()` returns `default`, or the value of
the catalog option `default-namespace`, and if that database does not exist, unqualified names fail with
a clear error naming the database and the `glue.id`. `USE <db>` must check existence through this
catalog's `namespaceExists` in both modes. `default-namespace` must be a single-level name; that is
checked in `initialize` without calling Glue, and existence is checked lazily on first use, so a
catalog can be configured while Glue is unreachable.

### Option validation and client defaults

- Unknown options under the catalog's own prefixes (`glue.`, `client.`) log a warning naming the key;
  they do not fail, because Spark passes every `spark.sql.catalog.<name>.*` option to the catalog,
  including options meant for Delta. Options under `client.credentials-provider.` are passed to the
  user's provider unchecked.
- Timeouts, retries and assume-role credential refresh use the AWS SDK v2 defaults (standard retry
  mode; the STS provider refreshes before expiry in the background). The README lists the concrete
  values for the SDK version gluecat is built against. STS failures name the catalog and the role ARN.
- `initialize` fails with a clear error on Spark versions below the supported floor (3.5.3 on the 3.5
  line) instead of failing later on the first non-Delta read.

### Identifiers and listing

Glue folds database and table names to lowercase when it stores them (Glue API reference, `TableInput`
and `CreateTable`), while Spark preserves the case the user typed. The catalog lowercases database and
table names before every Glue call and returns lowercase names in every `Identifier` and namespace it
hands back to Spark, so `acct_a.MyDb.MyTable` and `acct_a.mydb.mytable` are the same table in every
operation. Column names are left as they are (Delta keeps their case in its log).

`GetDatabases` and `GetTables` return at most 100 entries per call. Every listing follows `NextToken`
until it is exhausted; a test lists a database with more than 100 tables.

### Namespaces (Glue databases)

The full `SupportsNamespaces` contract for single-level namespaces: list, exists, load metadata, create
(with comment, location, properties), alter (set or remove properties, comment, location), drop (an
empty database; with `CASCADE`, drop its tables first, which never deletes table data). Multi-level
namespaces do not exist in Glue: `listNamespaces(ns)` for a non-empty `ns` returns nothing, and creating
one fails.

### Tables

| Operation | Delta tables | Other providers (Parquet, ORC, CSV, JSON, ...) |
|---|---|---|
| list, exists, load | yes; load returns Delta's `DeltaTableV2` | yes; load returns a `V1Table` that Spark reads through its V1 path |
| reads, time travel | Delta | Spark V1 read; only tables whose partitions are not tracked in Glue (Spark refuses those from a named catalog; as `spark_catalog` they would need the in-memory catalog to list partitions) |
| INSERT, MERGE, UPDATE, DELETE, OPTIMIZE, VACUUM, DESCRIBE HISTORY by name | Delta, on the loaded table | not supported (Spark refuses writes to a `V1Table` from a non-session catalog) |
| CREATE TABLE (external, with LOCATION) | yes | yes, metadata only |
| CREATE TABLE (managed, no LOCATION) | yes, location derived (below) | yes, metadata only |
| CTAS, `saveAsTable` | yes, with Delta's semantics | no |
| REPLACE TABLE, RTAS, `saveAsTable` overwrite | yes, with Delta's semantics | no |
| ALTER TABLE | Delta applies it in the log; column changes also re-sync the Glue entry's columns and schema; SET LOCATION updates the Glue location; properties stay in the log. Never through `spark_catalog` | properties, comment, columns, location in Glue |
| REFRESH TABLE | re-syncs the Glue entry's columns and schema from the Delta log (needs Glue write access) | no-op |
| RENAME | yes, Glue entry only, data stays in place | same |
| DROP | removes the Glue entry, never deletes data | same |
| DROP ... PURGE | fails with a clear error | same |

Path-based Delta operations (``delta.`s3://...` ``) don't involve any catalog and keep working as they do
today.

Managed tables get their location from the database's `LocationUri`, or from `<warehouse>/<db>.db` if
the database has none, plus the table name. With neither, creating a managed table fails. Managed
tables are dropped like external ones: the data stays.

Iceberg tables in Glue (`table_type = ICEBERG`) fail on load with an error that points to Iceberg's
`GlueCatalog`. Glue views are not listed and cannot be loaded. Hive SerDe tables without
`spark.sql.sources.provider` (written by Athena, Glue crawlers or Trino's Hive connector) fail on load
with a clear error.

### Compatibility with existing entries and readers

Entries written by this catalog must stay readable by Trino's Delta connector and by Spark's existing
Hive-client path. Entries those wrote (including copies of them in another account) must load in this
catalog unchanged. See "Glue entry format".

## Non-goals for 1.0.0

- Keeping the Glue columns in sync with the Delta log automatically after every commit. Spark and Trino
  read the Delta schema from `_delta_log`. Columns are written when the catalog creates or replaces an
  entry, on `ALTER TABLE` column changes, and on an explicit `REFRESH TABLE`, not after later commits.
- Making concurrent Delta writers safe. Delta's commit is the exclusive creation of
  `_delta_log/N.json` on S3; that belongs to Delta's LogStore. Delta-Spark's default S3 LogStore is safe
  for one driver only; a conditional-write (`If-None-Match`) LogStore is in review upstream
  (delta-io/delta#7827).
- Writing non-Delta tables, and reading non-Delta tables whose partitions are tracked in Glue.
- Iceberg (use Iceberg's `GlueCatalog`), Hudi, Hive SerDe tables, views, functions.
- Migrating registrations from one Glue catalog to another (done with separate tooling).
- Lake Formation, S3 credential vending, Spark 3.4 and older, Spark 3.5.0 to 3.5.2 (they reject a
  `V1Table` from a non-session catalog).
- Client-side caching of Glue responses (see "Risks").
- Tests against real AWS.

## Anti-goals

The project has failed, even if it works, if it:

- depends on the Hive metastore client or on patched Spark or Hive jars;
- needs DynamoDB or any other coordination service;
- produces Glue entries that Trino can no longer read as Delta tables;
- writes to any Glue catalog other than the configured one, in particular the one behind
  `spark_catalog`;
- triggers Delta's Unity Catalog code paths by naming classes `io.unitycatalog.*`;
- deletes table data on DROP.

## Constraints

- Spark 3.5.3+ (Scala 2.12 and 2.13, Java 8 bytecode) and Spark 4.0.0+ (Scala 2.13, Java 17 bytecode).
  3.5.3 is the first 3.5 release whose analyzer accepts a `V1Table` from a non-session catalog when the
  table does not track partitions in the catalog (checked: 3.5.0 to 3.5.2 lack it, 3.5.3 to 3.5.9 and
  4.0.0+ have it). Delta 3.3.x is the Delta line for Spark 3.5; its `DeltaCatalog` has the same
  `loadTable` and `isUnityCatalog` structure as Delta 4.4.1.
- Shared code must compile for Scala 2.12 and 2.13 and for Java 8. Spark-version-specific code lives in
  small per-version source directories (the internal classes used here differ between Spark 3.5 and
  4.x). The AWS SDK v2 still targets Java 8 (its README lists Java 8, 11, 17 and 21 as supported LTS
  releases).
- Delta is optional (`provided`). Without Delta on the classpath the catalog still works for non-Delta
  tables; Delta tables then fail on load with a clear error.
- The AWS SDK v2 (`glue`, `sts`) is a `provided` dependency, as in Iceberg's `iceberg-aws`: the jar contains
  no SDK classes and expects the SDK on the Spark classpath. gluecat compiles and tests against one SDK
  version, 2.24.6: the `bundle` version that `hadoop-aws` 3.4.1 depends on, and Hadoop 3.4.1 is the oldest
  Hadoop 3.4.x a supported Spark 4.x line bundles (Spark 4.0). Newer SDK versions on the classpath work
  (SDK 2.x stays backward compatible); older ones are not supported.
- On Spark 4.x, `hadoop-aws` (S3A) depends on the SDK v2 `bundle`, which already contains `glue` and `sts`.
  Spark 3.5 bundles Hadoop 3.3.4, whose S3A uses SDK v1, so users add the SDK v2 modules themselves
  (`--packages software.amazon.awssdk:glue:2.24.6,software.amazon.awssdk:sts:2.24.6`, or the `bundle`).
- The repository is private until 1.0.0 and public afterwards. No company, customer, account or bucket
  names in code, docs, tests or fixtures.
- A Maven Central version can never be deleted or overwritten. Semantic versioning from 1.0.0: the
  configuration property names and the Glue entry format are the public API.

## Decided

- Repository `github.com/sdaberdaku/gluecat`, private until 1.0.0.
- First release is 1.0.0 on Maven Central, artifacts as in the table above (Spark 3.5 with Scala 2.12
  and 2.13, Spark 4.x with Scala 2.13). Namespace `io.github.sdaberdaku`
  (verified via GitHub login on central.sonatype.com). Repository secrets already set: `GPG_PRIVATE_KEY`
  (passphrase-protected export of an RSA 4096 signing key, fingerprint
  `6A0C22C1941AE5948AF742070B390FD545465EBF`, published on keys.openpgp.org), `GPG_PASSPHRASE`,
  `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD` (a Central Portal user token).
- The catalog works both as a named catalog and as `spark_catalog`.
- Core catalog is format-agnostic; Delta support is layered on top.
- No automatic Glue schema sync after commits.
- Testing is CI-only, against MiniStack (`ministackorg/ministack`) pinned to an exact version, over a
  full matrix of Spark and Delta versions. MiniStack implements Glue databases, tables and partitions,
  checks `VersionId` on `UpdateTable`, keeps state per account and region, and maps `AssumeRole`
  sessions to the role's account, so two catalogs with two roles get two separate Glue stores.
- 2026-10-22: SDK relocation dropped (owner decision), see Constraints.
- 2026-10-22: `REFRESH TABLE` and `ALTER TABLE` column changes re-sync the Glue columns of Delta tables (owner
  decision); always on, no option.

## Design

### Components

`GlueCatalog` is the class users configure. It implements `TableCatalog`, `StagingTableCatalog` and
`SupportsNamespaces`, and never `CatalogExtension` (see "Using it as `spark_catalog`").

- On `initialize(name, options)` it builds one SDK v2 `GlueClient` from the options and creates the
  format-agnostic core catalog.
- If `org.apache.spark.sql.delta.catalog.DeltaCatalog` is on the classpath, it instantiates it by
  reflection, calls `setDelegateCatalog(core)` and `initialize(name, options)`, as `UCSingleCatalog`
  does. This instance is used for loading tables and for running Delta operations by path.
- Loading goes through `DeltaCatalog.loadTable`, which asks the core catalog for the entry and turns a
  Delta `V1Table` into a `DeltaTableV2` (time travel overloads included). Other tables pass through as
  `V1Table`.
- Namespace operations, listing, rename and drop go to the core catalog. `invalidateTable` is routed to the
  format support; Delta support implements the re-sync.
- Creates, replaces and alters are handled by `GlueCatalog` itself (below).

The core catalog maps Glue to Spark and nothing else: databases to namespaces, Glue tables to
`CatalogTable`, returned as `V1Table`. It contains no Delta code. Every `CatalogTable.identifier` it
returns carries the catalog name.

### Code structure (SOLID)

The code follows the SOLID principles, applied to the components above:

- Single responsibility. Each of these is its own class with one job:
  - `GlueCatalog`: implements the Spark interfaces and routes each call; no Glue or Delta logic of its
    own.
  - Core catalog: namespace and table operations against Glue.
  - Entry converter: pure functions between Glue `Table` / `Database` and Spark `CatalogTable` /
    namespace metadata, with no I/O, so the format rules ("Glue entry format") are unit-testable alone.
  - Client factory: catalog options to a configured `GlueClient` (region, endpoint, credentials chain,
    assume role, retries).
  - Delta support: path-based creates, replaces and staging.
  - Error mapping: AWS exceptions to Spark exceptions.
- Open/closed. Format behaviour plugs in behind a small format-support interface chosen at
  `initialize` (Delta when it is on the classpath, plain pass-through otherwise). Adding a format later
  adds an implementation, not branches in the core. Spark-version differences live in the per-version
  source directories behind one shim interface, never in `if (sparkVersion ...)` checks.
- Liskov substitution. `GlueCatalog` honours the contracts of `TableCatalog`, `StagingTableCatalog` and
  `SupportsNamespaces` exactly as Spark documents them (which exceptions, when `false` instead of an
  exception, no `null` results), and behaves the same as a named catalog and as `spark_catalog`.
  Contract tests check this for every operation.
- Interface segregation. Implement only the Spark interfaces that are supported in full (no
  `ViewCatalog`, `FunctionCatalog` or `CatalogExtension`). Internally, the core catalog talks to Glue
  through a narrow interface with only the Glue calls it uses.
- Dependency inversion. The core catalog depends on that narrow Glue interface, not on SDK classes.
  Production uses the SDK v2 implementation; unit tests use an in-memory one. `GlueCatalog` receives the
  client factory and the format support instead of creating them inline.

To keep this from turning into ceremony: an interface exists only where there are at least two
implementations (production and test count). The Glue interface (SDK and in-memory), format support
(Delta and pass-through) and the Spark shim (one per Spark line) qualify. Everything else is a plain
class.

### Why Delta's create path cannot be used as is

Delta's `CreateDeltaTableCommand` registers new or replaced tables through the wrapped catalog only when
the wrapped catalog's class name starts with `io.unitycatalog.` (`AbstractDeltaCatalog.isUnityCatalog`,
Delta 4.4.1). Otherwise it calls Spark's `SessionCatalog`, i.e. `spark_catalog`. So `GlueCatalog` must
never pass a catalog identifier to `DeltaCatalog.createTable` / `stageCreate` / `stageReplace` /
`stageCreateOrReplace`.

### Delta creates and replaces: run by path, then write Glue

For `provider = delta`, every create or replace runs as a path-based Delta operation on the table's
location, which Delta does without touching any catalog. Then `GlueCatalog` writes or updates the Glue
entry through the core catalog.

- `createTable` (CREATE TABLE without a query): resolve the location (explicit, or derived for managed
  tables), then call `DeltaCatalog.createTable` with the path identifier ``delta.`<location>` ``, then
  create the Glue entry, then return `loadTable(ident)`. If a Delta table already exists at the
  location, Delta's own rules apply: no columns given registers it as it is, columns given must match.
- `stageCreate` / `stageReplace` / `stageCreateOrReplace` (CTAS, RTAS, `saveAsTable`): return a staged
  table that wraps Delta's staged table for the path identifier. Spark writes the query result into it.
  On `commitStagedChanges`, Delta commits by path first (data and log), then `GlueCatalog` creates or
  updates the Glue entry. `abortStagedChanges` aborts Delta's stage and leaves Glue untouched. CTAS onto
  a location that already holds a Delta table then fails the way plain Delta's path CTAS fails.
- If the Glue write fails after Delta committed (after the SDK's own retries), the Delta table is valid
  at its location but has no Glue entry. The catalog throws a dedicated exception whose message
  contains the table identifier, the location and the Glue error, and states that the Delta commit
  succeeded. Re-running the CTAS is not safe (the location now holds a Delta table, so it fails); the
  recovery is `CREATE TABLE <ident> USING delta LOCATION '<location>'`, which is idempotent: it
  registers the existing table and fails only if an entry already exists. A test injects a Glue failure
  after the Delta commit and checks this exception and the recovery. For a replace, Delta's commit is a single atomic log entry; if the Glue update then fails, the
  Glue entry keeps its previous columns and properties, which no reader uses for Delta tables, and the
  error says so. Delta's own history (`RESTORE`) covers rolling back the data.

Faking the `io.unitycatalog.` prefix would be the shortcut to Delta's catalog-plugin create path, and
it stays an anti-goal. In Delta 4.4.1 that flag also turns on Unity Catalog-specific behaviour:
catalog-managed (catalog-owned commit) tables for managed creates (`allowCatalogManaged`), Unity
Catalog's managed-location rules, routing creates to a Unity Catalog REST client when the options
enable one, blocking the variant feature on Spark 4.0, and Unity Catalog table-ID property handling.
Each new Delta release can add more behind the same flag. The proper fix is upstream: a Delta change
that lets any catalog opt into `createTableFunc` (the `TODO` next to the check says the restriction
exists because of a `V2SessionCatalog` bug). Proposing that PR is a follow-up after 1.0.0; once a Delta
release has it, `GlueCatalog` can use it on that release and keep the by-path route for older ones.

The builder must verify early that `DeltaCatalog` accepts path identifiers in `createTable` and in the
staging calls on every Delta version in the matrix. Path identifiers need `spark.sql.runSQLOnFiles`
(true by default). If staging by path does not work on some versions, fall back to `createTable`
followed by Spark's non-atomic CTAS (create, then write into the loaded table) and document that CTAS is
not atomic on those versions.

### Alter

- Delta tables: pass `alterTable` to `DeltaCatalog.alterTable` with the catalog identifier. Delta applies
  most changes to the log. The builder audits which changes make Delta call back into a catalog
  (`super.alterTable` to our core is fine; anything reaching `spark.sessionState.catalog` is not) and
  routes those through the core catalog instead. A change set with any column change (add, delete,
  rename, type, comment, nullability, position) also re-syncs the Glue entry's columns and schema from
  the Delta log after Delta applies it, same as `REFRESH TABLE`.
- Other providers: the core catalog applies the changes to the Glue entry.
- Every Glue `UpdateTable` the catalog sends passes the `VersionId` it read, so a concurrent change makes
  the update fail with a retryable error instead of overwriting it. `SkipArchive` follows
  `glue.skip-archive` (Iceberg's default is true).

### Rename

Glue has no rename. `renameTable` creates the new entry from the old one, then deletes the old one. The
data does not move. If the target exists, it throws `TableAlreadyExistsException` before changing
anything. Not atomic: if the delete fails after the create, both entries exist and point at the same
location; the error names both, and dropping the old name (which never deletes data) finishes the
rename.

### Glue entry format

Start from the shape Spark's `HiveExternalCatalog` writes for data source tables, which Trino and
Spark's Hive-client path read today, with two additions for Delta so that Athena can read the tables
too:

- `TableType = EXTERNAL_TABLE`, parameter `EXTERNAL = TRUE`, for managed tables too (the catalog never
  deletes data, so Glue should not treat any of them as managed).
- `spark.sql.sources.provider = <provider>`, `spark.sql.create.version = <Spark version>`,
  `spark.sql.partitionProvider = filesystem`.
- `spark.sql.sources.schema = <schema JSON>`, split into `spark.sql.sources.schema.numParts` and
  `spark.sql.sources.schema.part.N` when it exceeds Spark's length threshold. Partition columns in
  `spark.sql.sources.schema.numPartCols` / `spark.sql.sources.schema.partCol.N`.
- The real location in the SerDe parameter `path`, which Trino's Delta connector and Spark's Hive path
  read first.
- Delta tables: `StorageDescriptor.Location` = the real location (not Spark's `-__PLACEHOLDER__`
  suffix) and parameter `table_type = DELTA`, which is what Athena's Delta support needs (Athena
  documents `LOCATION` plus `TBLPROPERTIES ('table_type' = 'DELTA')`). The placeholder exists so that
  the Hive metastore does not manage a directory at the table location; Glue never touches the location,
  and the readers above take the SerDe `path` first, so the real location is safe. SerDe, input and
  output formats as Spark writes them for non-Hive-compatible providers (`LazySimpleSerDe`,
  `SequenceFileInputFormat`, `HiveSequenceFileOutputFormat`).
- Other providers: exactly what `HiveExternalCatalog.createDataSourceTable` would write, including its
  choice between the Hive-compatible form (real SerDe, input and output formats, columns and location
  for providers such as Parquet and ORC, per Spark's `HiveSerDe.sourceToSerDe`) and the Spark-specific
  form with the placeholder location.
- `StorageDescriptor.Columns`: the table's columns at create, replace, `ALTER TABLE` column change or
  `REFRESH TABLE` time (name and Spark `catalogString` type). No `PartitionKeys`.
- User table properties as Glue parameters.

`classification` is not set: Athena's Delta documentation does not use it, and it is a Glue crawler and
ETL convention.

When reading, accept at least these variants:

- an entry written by Spark 3.5's Hive path for a Delta CTAS: columns `col array<string>`,
  `spark.sql.sources.schema = {"type":"struct","fields":[]}`, `spark.sql.partitionProvider = catalog`;
- an entry whose columns and properties Delta rewrote after a `REPLACE` / overwrite: real columns plus
  `delta.lastUpdateVersion`, `delta.lastCommitTimestamp`, `delta.minReaderVersion`,
  `delta.minWriterVersion`;
- schemas stored in parts;
- both a placeholder and a real `StorageDescriptor.Location`, with the SerDe `path` taking precedence;
- non-Delta data source tables written by Spark's Hive path, in both of its forms.

Map the internal `spark.sql.*` keys back into `CatalogTable` fields the way `HiveExternalCatalog`
restores them, and keep them out of the user-visible properties.

The entry format is public API from 1.0.0. Rules that are part of it:

- Required for loading: `spark.sql.sources.provider`, and a location (SerDe `path`, else
  `StorageDescriptor.Location` without the placeholder suffix). Everything else is optional when
  reading; a Delta entry with no schema loads, because Delta reads the schema from its log.
- Unknown parameters (written by other tools, for example `trino_*` or `delta.*` keys) are preserved on
  every update: an `UpdateTable` starts from the entry as read and changes only the keys the operation
  owns.
- Schema parts are written with Spark's threshold (`spark.sql.sources.schemaStringLengthThreshold`,
  default 4000 characters), numbered from 0, and read in order; a missing part is a load error naming
  the table.
- Names follow "Identifiers and listing" (lowercase).
- Malformed entries (bad schema JSON, missing location, a part count that does not match) fail on load
  with an error that names the table and the problem.

### Delta's UpdateCatalog hook

Delta registers its `UpdateCatalog` post-commit hook for every transaction on a table loaded by name, so
for every write by name into a Delta table of our catalog. It only runs when
`spark.databricks.delta.catalog.update.enabled` is true (default false). When it runs, it updates the
table through `spark.sessionState.catalog` (`tableExists`, `alterTable`, `alterTableDataSchema`), i.e.
through `spark_catalog`. If `spark_catalog` holds a table with the same `db.table`, it could update that
entry, which is an anti-goal.

The builder tests what the hook does with our tables when the setting is on and `spark_catalog` has a
same-named table. If it can touch the session catalog's entry, `GlueCatalog.initialize` fails with a
clear error while the setting is on. If it is harmless, a README note is enough.

### Errors

One mapping, used by every code path (core, Delta support, staging, rename), lives in the error
mapping class:

| Glue / AWS error | Spark exception | Message includes |
|---|---|---|
| `EntityNotFoundException` on a table call | `NoSuchTableException` | catalog name, identifier |
| `EntityNotFoundException` on a database call | `NoSuchNamespaceException` | catalog name, namespace |
| `AlreadyExistsException` on a table call | `TableAlreadyExistsException` | catalog name, identifier |
| `AlreadyExistsException` on a database call | `NamespaceAlreadyExistsException` | catalog name, namespace |
| `ConcurrentModificationException` (stale `VersionId`) | a retryable exception of this library | catalog name, identifier, expected `VersionId` |
| `AccessDeniedException`, STS errors | the AWS exception, wrapped | catalog name, `glue.id`, role ARN if any |
| other AWS errors | the AWS exception, wrapped | catalog name, `glue.id`, operation |
| Glue write failed after a Delta commit | the dedicated exception above | identifier, location, recovery statement |

## Testing

All tests run in CI. Integration tests run Spark in local mode against MiniStack started as a service
container; Delta data lives on local disk, since the catalog only stores the location string.

The first build step proves the Delta create path before anything else is built on it: on one Spark and
Delta pair, run `CREATE TABLE ... USING delta LOCATION`, SQL CTAS, `DataFrameWriter.saveAsTable` with
`option("path", ...)`, `INSERT` and `MERGE INTO` by name, a path-based `MERGE`, and read the table back
by name. Check the Glue entry's shape and that nothing was registered in `spark_catalog`. If this does
not work, stop and revise this design.

Then:

- Unit tests for the Glue to `CatalogTable` conversion in both directions, with versioned golden
  fixtures (generic names) for the entry variants above, schema-part boundaries, preserved unknown
  parameters, and malformed entries with their expected error messages.
- Unit tests for the core catalog against the in-memory Glue implementation, and contract tests for
  `GlueCatalog` against Spark's documented `TableCatalog` / `SupportsNamespaces` behaviour.
- One integration test per row of the operations table, for Delta and for Parquet, including the
  failure rows (PURGE, writes to Parquet, partition-tracked Parquet, Iceberg, views, Hive SerDe tables).
- Two catalogs on two MiniStack accounts through `AssumeRole`, both holding the same `db.table`, to show
  they stay separate. A library-level request observer records every Glue request (operation,
  `CatalogId`), and the isolation tests assert that no request carried a `CatalogId` other than the
  catalog's own.
- A `spark_catalog` backed by a separate Glue account in the same session, to show that no operation on
  our catalogs, including with the `UpdateCatalog` hook on, touches it.
- `VersionId` conflict on two concurrent table alters (property and schema changes). Glue databases
  have no `VersionId`, so concurrent namespace alters are last-writer-wins; the README says so.
- Credentials: default chain, assume role, custom provider class, and IRSA web identity through the `sts`
  module (simulate IRSA with a token file and MiniStack's STS, if it supports
  `AssumeRoleWithWebIdentity`; otherwise at least check that the web identity provider class loads).
- Integration tests run against the built jar, not only the classes. A packaging test opens the jar and
  checks that it contains no `software/amazon/`, `org/reactivestreams/` or
  `META-INF/services/software.amazon.*` entries and no signature files, and that `GlueCatalog.class` is
  present; the published POM lists `glue` and `sts` as `provided` and nothing from the SDK at compile or
  runtime scope.
- A Trino container with the Delta Lake connector on the same MiniStack Glue reads, by name, a Delta table
  gluecat created, including rows added by a later INSERT and a partitioned CTAS.
- Spark 3.5 with the Glue Hive client (SDK v1, patched Hive) on the same MiniStack Glue reads gluecat's
  Delta tables by name, and gluecat loads by name the Delta and Parquet tables that client created.
- Both catalog modes (named catalog and `spark_catalog`) for every integration test, plus the default
  namespace cases: missing `default` gives the clear error, `default-namespace` and `USE <db>` work.

CI matrix: every artifact in the table above (Spark minor and Scala version, latest patch of each Spark
minor, plus 3.5.3 as the 3.5 floor), crossed with every Delta release line that supports that Spark
minor according to Delta's compatibility table, crossed with every JDK that Spark minor supports. Keep
the matrix in one place so adding a Spark, Delta or JDK release is a one-line change.

The full matrix is about 40 jobs; at roughly 5 minutes each that is about 200 runner minutes per run,
and GitHub Free gives a private repository 2,000 minutes a month. So, while the repository is private,
pull requests and pushes to `main` run a smoke matrix (the oldest and newest Spark/Delta/JDK combination
of each artifact), and the full matrix runs on tags and on manual dispatch. Once the repository is
public (Actions minutes are free for public repositories), pushes to `main` run the full matrix too.

## Release

- Maven with `scala-maven-plugin` and `central-publishing-maven-plugin`, triggered by pushing a tag
  `vX.Y.Z`, after the full matrix passes on that commit. One release publishes every artifact in the
  table above, all with the same version, as one Central deployment. Sources jar, javadoc jar (scaladoc), GPG
  signatures and the POM metadata Central requires (license, developer, SCM).
- The same jar is attached to a GitHub Release for the tag.
- Documentation is published to GitHub Pages from `docs/site/` by `.github/workflows/pages.yml`; the site goes live
  once the repository is public.
- Flip the repository to public before tagging v1.0.0.

## Known limitations to document in the README

- One writer per Delta table, until Delta-Spark gets a conditional-write LogStore.
- Glue columns are synced at create, replace, `ALTER TABLE` column changes and `REFRESH TABLE`; not after schema evolution inside writes
  (`mergeSchema`, `MERGE ... WITH SCHEMA EVOLUTION`), which needs a `REFRESH TABLE`. `REFRESH TABLE` on a Delta
  table needs Glue write access.
- Non-Delta tables are read-only, and only those whose partitions are not tracked in Glue.
- DROP never deletes data; PURGE is not supported. This differs from Spark's managed-table semantics:
  dropped managed tables leave their directories under `<warehouse>/<db>.db/` or the database location,
  and repeated create/drop cycles (test suites, scratch CTAS) accumulate unreferenced data there that
  has to be cleaned up separately.
- RENAME is not atomic.
- The `UpdateCatalog` hook behaviour, per the test result.
- Tested against MiniStack only: IAM policies, Glue resource policies and cross-account `CatalogId`
  access are not exercised against real AWS. Cross-account behaviour is tested by request shape (which
  `CatalogId` and which STS identity each call uses), not by AWS authorization outcomes.
- Concurrent namespace alters are last-writer-wins (Glue databases have no `VersionId`).

## Risks

- Spark's internal classes used here (`CatalogTable`, `V1Table`) and Delta's internals (`DeltaCatalog`
  wrapping, path identifiers, staged tables) can change between minor versions. Per-Spark-minor
  artifacts and per-version source directories absorb that; the matrix finds where it is needed.
- Some behaviour differs between Spark 3.5 and 4.x in ways the source checks above do not cover (for
  example `CatalogManager.setCurrentNamespace` was rewritten in 4.2). Every claim in this design that
  was checked against one Spark version must be covered by a test that runs on all of them.
- SDK version drift between gluecat's compile version and the bundle on the user's classpath. Mitigation:
  compile and test against the oldest supported bundle version (see Constraints), so the newer SDKs
  that later Hadoop and Spark lines bring only add APIs; an older SDK is documented as unsupported.
- Jobs that check `tableExists` very often (for example once per streaming micro-batch per table) cause
  one `GetTable` call each. If Glue throttling shows up, add a short-lived cache then, not before.
- MiniStack is young and changes daily; a bug there can look like a bug here. Pin the version and
  upgrade on purpose.

## My calls (the builder may revisit)

- Scala (2.12 and 2.13) and Maven, Apache 2.0 license.
- Package `io.github.sdaberdaku.gluecat`, class `GlueCatalog`.
- One artifact per Spark minor and Scala version, named `gluecat-spark-<minor>_<scala>`.
- Spark 3.5 floor at 3.5.3; Spark 3.4 left out (its last release was 3.4.4 in 2024).
- No auto-creation of the `default` database; `default-namespace` option.
- Managed tables are supported but DROP never deletes data, for any table; PURGE fails.
- All entries are written as `EXTERNAL_TABLE`.
- `REFRESH TABLE` re-syncs Glue columns for Delta tables (always on).
- Columns written at create, replace, `ALTER TABLE` column change and `REFRESH TABLE`; SerDe `path` mirrored from Spark; for Delta, the real
  `StorageDescriptor.Location` and `table_type = DELTA` instead of Spark's placeholder, for Athena.
- Database and table names lowercased in both directions.
- SDK default HTTP client; SDK `provided`.
- Full CI matrix on tags and manual dispatch while the repository is private.
- `VersionId` on every `UpdateTable`, `SkipArchive` from `glue.skip-archive`.
- `GlueCatalog` implements `StagingTableCatalog` and runs Delta creates and replaces by path.
- RENAME implemented as create plus delete.
- If the `UpdateCatalog` hook can reach `spark_catalog`, the catalog refuses to initialize while it is on.
- Hive SerDe tables, views and Iceberg tables fail on load with clear errors.

## Left to the builder

- Error messages and logging.
- Exact Iceberg-compatible property names and defaults.
- How the matrix is expressed in CI.
