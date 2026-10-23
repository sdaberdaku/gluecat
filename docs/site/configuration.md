# Configuration

Options go under `spark.sql.catalog.<name>.`.

| Option | Default | Meaning |
|---|---|---|
| `glue.id` | not set: the caller's account | Glue catalog ID (AWS account ID), sent as `CatalogId` on every Glue call. With `client.region` it identifies the Glue Data Catalog: one per account and region. |
| `glue.endpoint` | SDK default | Endpoint override for the Glue client only (not STS). |
| `glue.skip-archive` | `true` | `SkipArchive` on every Glue `UpdateTable`. |
| `client.region` | SDK default region chain | Region of the Glue client, and of STS unless `client.assume-role.region` is set. With `glue.id` it identifies the Glue Data Catalog. Also the derived `fs.s3a.endpoint.region` (see below). |
| `client.credentials-provider` | not set | Class name of an SDK v2 `AwsCredentialsProvider`. Instantiated through a static `create(java.util.Map)`, else a static `create()`, else the no-arg constructor. |
| `client.credentials-provider.<key>` | not set | Passed, without the prefix, to the provider's `create(java.util.Map)`. |
| `client.assume-role.arn` | not set | IAM role to assume through STS for every Glue call. |
| `client.assume-role.external-id` | not set | External ID for the assume-role call. |
| `client.assume-role.session-name` | `gluecat` | Role session name. |
| `client.assume-role.region` | `client.region` | STS region. |
| `client.assume-role.timeout-sec` | `3600` | Session duration in seconds; a positive integer. |
| `hadoop.<key>` | not set | Hadoop configuration override for the tables of this catalog, with the prefix removed. See [Per-table Hadoop options](#per-table-hadoop-options). |
| `http-client.type` | SDK default resolution | `apache` or `urlconnection`: the HTTP client of the Glue and STS clients. Unset: inferred from the other `http-client.*` keys (see below). |
| `http-client.proxy-endpoint` | not set | Proxy URL (apache client; also accepted for urlconnection). |
| `http-client.proxy-use-system-property-values` | SDK default | Read the proxy from the JVM system properties. |
| `http-client.proxy-use-environment-variable-values` | SDK default | Read the proxy from the environment variables. |
| `http-client.urlconnection.socket-timeout-ms` | SDK default | Socket timeout of the urlconnection client. |
| `http-client.urlconnection.connection-timeout-ms` | SDK default | Connection timeout of the urlconnection client. |
| `http-client.apache.socket-timeout-ms` | SDK default | Socket timeout of the apache client. |
| `http-client.apache.connection-timeout-ms` | SDK default | Connection timeout of the apache client. |
| `http-client.apache.connection-acquisition-timeout-ms` | SDK default | Time to wait for a pooled connection. |
| `http-client.apache.connection-max-idle-time-ms` | SDK default | Maximum idle time of a pooled connection. |
| `http-client.apache.connection-time-to-live-ms` | SDK default | Maximum lifetime of a pooled connection. |
| `http-client.apache.expect-continue-enabled` | SDK default | `Expect: 100-continue` on requests. |
| `http-client.apache.max-connections` | SDK default | Connection pool size. |
| `http-client.apache.tcp-keep-alive-enabled` | SDK default | TCP keep-alive. |
| `http-client.apache.use-idle-connection-reaper-enabled` | SDK default | Background reaper for idle connections. |
| `warehouse` | not set | Location of databases created without one (`<warehouse>/<db>.db`), and of managed tables in databases without a location: `<warehouse>/<db>.db/<table>`. |
| `default-namespace` | `default` | Database of unqualified table names. Named catalogs only: as `spark_catalog`, Spark takes the current namespace from its built-in session catalog (`default` until `USE`). |

Unknown options under `glue.`, `client.` and `http-client.` log a warning and are ignored (Spark passes every
`spark.sql.catalog.<name>.*` option to the catalog, including options meant for Delta). Blank values count as not set.
The catalog never creates a database: if the default database does not exist, unqualified names fail with an error
that names the database and the `glue.id`.

## HTTP client

`http-client.*` uses Iceberg's names and applies to the Glue client and the STS client of the catalog.

- With no `http-client.*` key, the SDK picks its own HTTP client, as in 1.0.0.
- `http-client.type` selects `apache` or `urlconnection`. Without it, `http-client.apache.*` or a proxy key implies
  `apache` (Iceberg's default, so shared Iceberg settings take effect), and only `http-client.urlconnection.*` keys
  imply `urlconnection`. Keys for both client types without a type are an error at `initialize`.
- A key for the other client type than the selected one is ignored with a warning.
- The clients are optional dependencies: add `software.amazon.awssdk:apache-client` or
  `software.amazon.awssdk:url-connection-client` (2.24.6) to the classpath. Selecting a type whose jar is missing
  fails at `initialize` with a message naming the artifact.

## Per-table Hadoop options

Every `hadoop.<key>` option becomes the Hadoop configuration key `<key>` for the tables of this catalog: loads, reads,
writes, creates and replaces. The value is not validated; Hadoop owns it. Only `fs.*` and `dfs.*` keys reach Delta
(it filters storage properties by those prefixes); other keys reach non-Delta data sources only.

### Derived S3 defaults

A catalog with `client.assume-role.arn` and no `client.credentials-provider` makes S3A assume the same role, with no
further configuration (Iceberg's `AssumeRoleAwsClientFactory` does the same for its S3 access). gluecat derives:

| Hadoop key | Value |
|---|---|
| `fs.s3a.aws.credentials.provider` | `org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider` |
| `fs.s3a.assumed.role.arn` | `client.assume-role.arn` |
| `fs.s3a.assumed.role.session.name` | `client.assume-role.session-name` |
| `fs.s3a.assumed.role.session.duration` | `client.assume-role.timeout-sec`, as `<n>s` |
| `fs.s3a.assumed.role.external.id` | `client.assume-role.external-id`, when set. Hadoop 3.4 (Spark 4.x) reads it; Hadoop 3.3 (Spark 3.5) ignores it |
| `fs.s3a.endpoint.region` | `client.region`, when set |
| `fs.s3a.assumed.role.credentials.provider` | the session's `fs.s3a.aws.credentials.provider`, only when the session sets that key, does not set this one, and its provider is not `AssumedRoleCredentialProvider` itself |

The last row keeps IRSA working: Hadoop's own default for the base credentials cannot do web identity. gluecat reads
the session's S3A settings once, at `initialize`; Hadoop's built-in defaults do not count as set by you.

gluecat never derives `fs.s3a.assumed.role.sts.endpoint` or `fs.s3a.assumed.role.sts.endpoint.region`. On Hadoop 3.4
the endpoint takes a host name (`sts.eu-central-1.amazonaws.com`), not a URL, and Hadoop refuses the region without
the endpoint. A catalog that needs a regional or private STS endpoint sets both under `hadoop.`.

### Precedence

1. An explicit `hadoop.<key>` wins over a derived value for the same key.
2. A derived value wins over the session's base `fs.s3a.*` keys.
3. The session configuration is the fallback.
4. Session per-bucket keys (`fs.s3a.bucket.<name>.*`) win over all of the above, because S3A copies them over the
   base keys when it opens the bucket. Existing per-bucket setups keep working unchanged.

The opt-out is to set `hadoop.fs.s3a.aws.credentials.provider` yourself; there is no separate switch. A catalog with
no role, or with a custom `client.credentials-provider`, derives nothing (S3A cannot reuse an arbitrary SDK provider)
and still honours explicit `hadoop.*` keys. On Spark 3.5 (Hadoop 3.3, SDK v1) a catalog without a role needs
`hadoop.fs.s3a.endpoint.region` (or the session's) to avoid a 301 from S3; with a role the derived key covers it.

### One bucket, one way

Hadoop caches `FileSystem` instances by scheme, bucket and user, not by configuration. The first access to a bucket
in a JVM fixes its credentials and endpoint for every later access, in the driver and in each executor. gluecat
opens a table's location with the catalog's configuration before its own Delta operations and relies on the cache
being enabled (Hadoop's default). The rule that follows: reach a bucket one way only, through one catalog's role or
through the session configuration, never both. Two catalogs with different roles on the same bucket share the first
instance.

Direct path access (`` delta.`s3://...` ``, `spark.read.parquet`) never reaches a catalog and always uses the session
configuration. See [How it works](how-it-works.md#per-catalog-hadoop-configuration).

### What is visible

gluecat writes none of these options to Glue or to the Delta log. Spark and Delta 3.3 and 4.0 show storage properties
as `option.<key>` in `SHOW TBLPROPERTIES` and `DESCRIBE EXTENDED` of the session that loaded the table (Delta 4.3 and
later hide `fs.*`). Prefer roles over static keys under `hadoop.*`: a static secret would show up there.

## Credentials

In order of precedence:

1. `client.credentials-provider`: your provider class.
2. `client.assume-role.arn`: an STS assume-role provider. Its base credentials come from the SDK default chain. The
   SDK refreshes the session on the thread of the first Glue call in the 5 minutes before it expires; there is no
   background refresh thread.
3. The SDK default credentials chain, including web identity (IRSA on EKS).

Each catalog builds its own default chain and resolves it at its first Glue call. A `glue.id` of another account
also works without a role if that account's Glue resource policy allows the caller.

- IRSA (web identity) needs the `sts` module on the classpath, like assume role.
- With SDK 2.24.6, assume-role and web identity calls go to the regional public STS endpoint
  (`sts.<region>.amazonaws.com`); `AWS_ENDPOINT_URL` and `AWS_ENDPOINT_URL_STS` are ignored (later SDK versions
  read them). With a local emulator, `glue.endpoint` points Glue at it, but `client.assume-role.arn` still reaches
  the real STS.
- STS VPC interface endpoints work only with private DNS enabled.
- To use several accounts and regions, see [Across AWS accounts](multi-account.md).

## AWS SDK

gluecat is built and tested against SDK 2.24.6, the `bundle` version that `hadoop-aws` 3.4.1 depends on (Hadoop 3.4.1
is the oldest Hadoop that a supported Spark 4.x line bundles). `glue` and `sts` are `provided` dependencies, as in
Iceberg's `iceberg-aws`; `apache-client` and `url-connection-client` are optional ones. Newer 2.x versions on the classpath are expected to work, since the SDK keeps backward
compatibility within 2.x, but CI does not test them; older versions are not supported.

gluecat sets the standard retry mode (3 attempts), which overrides `AWS_RETRY_MODE` (the SDK's own default is the
legacy mode), and otherwise keeps the SDK defaults: no API call timeout, and the SDK's default HTTP client resolution
unless you set `http-client.*`.
Assume-role sessions last `client.assume-role.timeout-sec` (3600 s by default) and refresh on the calling thread.
Spark never closes catalog plugins and creates new ones for new sessions (`newSession`, Spark Connect and Thrift
server sessions, each streaming query), so gluecat starts no background refresh that would outlive them.
