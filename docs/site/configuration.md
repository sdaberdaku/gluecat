# Configuration

Options go under `spark.sql.catalog.<name>.`.

| Option | Default | Meaning |
|---|---|---|
| `glue.id` | not set: the caller's account | Glue catalog ID (AWS account ID), sent as `CatalogId` on every Glue call. |
| `glue.endpoint` | SDK default | Endpoint override for the Glue client only (not STS). |
| `glue.skip-archive` | `true` | `SkipArchive` on every Glue `UpdateTable`. |
| `client.region` | SDK default region chain | Region of the Glue client, and of STS unless `client.assume-role.region` is set. |
| `client.credentials-provider` | not set | Class name of an SDK v2 `AwsCredentialsProvider`. Instantiated through a static `create(java.util.Map)`, else a static `create()`, else the no-arg constructor. |
| `client.credentials-provider.<key>` | not set | Passed, without the prefix, to the provider's `create(java.util.Map)`. |
| `client.assume-role.arn` | not set | IAM role to assume through STS for every Glue call. |
| `client.assume-role.external-id` | not set | External ID for the assume-role call. |
| `client.assume-role.session-name` | `gluecat` | Role session name. |
| `client.assume-role.region` | `client.region` | STS region. |
| `client.assume-role.timeout-sec` | `3600` | Session duration in seconds; a positive integer. |
| `warehouse` | not set | Location of databases created without one (`<warehouse>/<db>.db`), and of managed tables in databases without a location: `<warehouse>/<db>.db/<table>`. |
| `default-namespace` | `default` | Database of unqualified table names. Named catalogs only: as `spark_catalog`, Spark takes the current namespace from its built-in session catalog (`default` until `USE`). |

Unknown options under `glue.` and `client.` log a warning and are ignored (Spark passes every
`spark.sql.catalog.<name>.*` option to the catalog, including options meant for Delta). Blank values count as not set.
The catalog never creates a database: if the default database does not exist, unqualified names fail with an error
that names the database and the `glue.id`.

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

## AWS SDK

gluecat is built and tested against SDK 2.24.6, the `bundle` version that `hadoop-aws` 3.4.1 depends on (Hadoop 3.4.1
is the oldest Hadoop that a supported Spark 4.x line bundles). `glue` and `sts` are `provided` dependencies, as in
Iceberg's `iceberg-aws`. Newer 2.x versions on the classpath are expected to work, since the SDK keeps backward
compatibility within 2.x, but CI does not test them; older versions are not supported.

gluecat sets the standard retry mode (3 attempts), which overrides `AWS_RETRY_MODE` (the SDK's own default is the
legacy mode), and otherwise keeps the SDK defaults: no API call timeout, the SDK's default HTTP client resolution.
Assume-role sessions last `client.assume-role.timeout-sec` (3600 s by default) and refresh on the calling thread.
Spark never closes catalog plugins and creates new ones for new sessions (`newSession`, Spark Connect and Thrift
server sessions, each streaming query), so gluecat starts no background refresh that would outlive them.
