# Across AWS accounts

This page explains how to use one Spark application with several Glue Data Catalogs in several AWS accounts and
regions. Each catalog assumes its own IAM role. The account IDs (`111111111111`, `222222222222`), catalog names
(`acct_a`, `acct_b`) and buckets (`bucket-a`, `bucket-b`) are examples.

## 1. The identity model

- The Spark application has one base identity. gluecat's Glue and STS clients find it with the AWS SDK default
  credentials chain (IRSA on EKS, an instance profile, a profile on a laptop). S3A has its own chain, set by the
  session's `fs.s3a.aws.credentials.provider` (Hadoop's default list has no profile provider); for the same
  application the two should resolve to the same identity.
- Each catalog assumes one role from that identity. The role belongs to the account that owns the Glue catalog and the
  data.
- Each client uses a different credential path:

| Client | Credentials |
|---|---|
| STS call of gluecat (assume role) | The base identity, from the SDK default chain. |
| Glue | The catalog's role (`client.assume-role.arn`). |
| STS call of S3A (assume role) | The session's effective `fs.s3a.aws.credentials.provider`, minus `AssumedRoleCredentialProvider` (derived into `fs.s3a.assumed.role.credentials.provider`). For IRSA the session must name the web-identity provider. |
| S3A (data files) | The catalog's role, through the [derived defaults](configuration.md#derived-s3-defaults), unless you set another role under `hadoop.`. |

An Iceberg catalog in the same application does the same with its own client factory
(`AssumeRoleAwsClientFactory`), configured separately.

```text
                      Spark application (one JVM, one base identity)
                    arn:aws:iam::999999999999:role/spark-app (IRSA)
                       |                                   |
            catalog acct_a                          catalog acct_b
   glue.id 111111111111, eu-central-1       glue.id 222222222222, us-east-1
                       |                                   |
        sts:AssumeRole (base identity)           sts:AssumeRole (base identity)
                       v                                   v
   role 111111111111:role/glue-access       role 222222222222:role/glue-access
          |                    |                   |                    |
   Glue catalog         S3 (bucket-a)        Glue catalog         S3 (bucket-b)
   111111111111                              222222222222
   eu-central-1                              us-east-1
```

A catalog never calls any Glue catalog other than its own `glue.id` and region, and never uses another catalog's role.

## 2. The IAM pieces and who owns each

Per catalog, five documents exist. They sit in different accounts, so they usually have different owners.

1. **The base identity's policy** (in the application's account): allows `sts:AssumeRole` on each catalog role. Without
   it STS answers `AccessDenied` before Glue is called.
2. **The role's trust policy** (in the catalog's account): names the base identity as principal. If you set
   `client.assume-role.external-id`, add a `sts:ExternalId` condition.
3. **The role's Glue policy** (in the catalog's account): the ten actions from
   [Operations](operations.md#iam-permissions).
4. **The role's S3 policy** (in the catalog's account): `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`,
   `s3:AbortMultipartUpload` and `s3:ListMultipartUploadParts` on the data prefixes, `s3:ListBucket`,
   `s3:ListBucketMultipartUploads` and `s3:GetBucketLocation` on the buckets.
5. **Bucket policies**, only when the bucket lives in another account than the role: the bucket policy must allow the
   role as well.

Glue also supports resource policies: a `glue.id` of another account works without a role if that account's Glue
resource policy allows the caller. A role is the better route. It covers Glue and S3 with one identity per catalog,
needs no policy in the other account's Glue, and gives each catalog its own audit trail (the role session name is
`client.assume-role.session-name`, `gluecat` by default). Without a role, S3A uses the session configuration, so
Glue and S3 access can differ.

## 3. Regions

A Glue Data Catalog is identified by the pair account ID and region. One AWS account has one catalog per region, and
each pair is its own gluecat catalog, configured with `glue.id` and `client.region`. Two regions of the same account
are two catalogs, even with the same role (IAM roles are global):

```
spark.sql.catalog.acct_a_eu                          = io.github.sdaberdaku.gluecat.GlueCatalog
spark.sql.catalog.acct_a_eu.glue.id                  = 111111111111
spark.sql.catalog.acct_a_eu.client.region            = eu-central-1
spark.sql.catalog.acct_a_eu.client.assume-role.arn   = arn:aws:iam::111111111111:role/glue-access

spark.sql.catalog.acct_a_us                          = io.github.sdaberdaku.gluecat.GlueCatalog
spark.sql.catalog.acct_a_us.glue.id                  = 111111111111
spark.sql.catalog.acct_a_us.client.region            = us-east-1
spark.sql.catalog.acct_a_us.client.assume-role.arn   = arn:aws:iam::111111111111:role/glue-access
```

- `client.region` sets the region of the Glue client and, unless `client.assume-role.region` is set, of the STS client.
  `client.assume-role.region` pins STS to another region, for example one close to the application.
- A catalog that sets `client.region` ignores the application's default region (`AWS_REGION`, the profile). Without
  `client.region`, the SDK's default region chain applies.
- `client.region` does not set the region of S3 requests: gluecat does not derive `fs.s3a.endpoint.region`, and S3A's
  own region handling applies on every Hadoop line (Hadoop 3.3 uses global bucket access when no endpoint is set;
  hadoop-aws 3.4.1 falls back across regions only while no region is set; 3.4.2 and later have
  `fs.s3a.cross.region.access.enabled`, on by default). A bucket in another region than its Glue catalog therefore
  needs nothing. Set `hadoop.fs.s3a.endpoint.region` only with a custom endpoint that names no region, or to pin a
  region.

## 4. Sessions

- An assumed-role session lasts `client.assume-role.timeout-sec` (3600 seconds by default). The role's maximum session
  duration must allow it, or STS refuses the call. When the base identity is itself an assumed-role session (role
  chaining), AWS caps the chained session at one hour.
- The SDK refreshes the session on the thread of the first Glue call in the 5 minutes before it expires. There is no
  background thread. S3A assumes its own session for the same duration (`fs.s3a.assumed.role.session.duration`) and
  manages its refresh itself.
- Each catalog resolves its credentials at its first Glue call, not at `initialize`. A role that cannot be assumed
  fails on the first statement.
- A revoked role or an expired session shows as an access denied or expired-token error from STS, Glue or S3 on the
  next call after the session ends. Every gluecat error for a Glue call names the catalog, the `glue.id` and, when the
  catalog has one, the role ARN, in one message: `Catalog acct_b (glue.id=222222222222): access denied: Glue
  GetDatabase (catalog 222222222222) (role arn:aws:iam::222222222222:role/glue-access): <AWS message>`.

## 5. S3 in detail

With `client.assume-role.arn` set, the catalog's tables are read and written under the same role. The
[derived defaults table](configuration.md#derived-s3-defaults) lists what gluecat sets. To opt out, set
`hadoop.fs.s3a.aws.credentials.provider` yourself.

Add explicit `hadoop.*` keys when S3 needs something the role does not give: a custom endpoint
(`hadoop.fs.s3a.endpoint`), path-style access, requester pays, server-side encryption settings. They win over the
derived values for the same key.

The Hadoop `FileSystem` cache is keyed by scheme, bucket and user, not by configuration, so the first access to a
bucket in a JVM fixes its credentials and endpoint. gluecat primes the driver's cache with the catalog's
configuration before its own Delta operations (executor-side Delta work for those operations is not covered; see
[Limitations](limitations.md)). The rule for you is one role per bucket: reach a bucket through one catalog's role,
or through the session configuration, never both, and do not point two catalogs with different roles at the same
bucket. Direct path access (`` delta.`s3://bucket-a/...` ``, `spark.read.parquet`) never reaches a catalog: it uses
whatever the cache already holds for that bucket, else the session configuration.

Streaming is the exception inside one catalog: `writeStream.toTable` on a Delta table by name (and, on Delta 3.3,
`readStream.table`) does not receive the catalog's Hadoop options. Spark starts the sink with the provider and
location only, and Delta builds its log without storage properties, so the session configuration is used: the
streaming job reaches the bucket with the session identity while batch work uses the role. For the buckets that
streaming writes, set per-bucket session keys (`fs.s3a.bucket.<name>.*`) with the role, or write with `foreachBatch`
and batch writes by name.

### One role for Glue, another for S3

`client.assume-role.arn` is used for Glue. To use another role for the data, set it under `hadoop.`:

```
spark.sql.catalog.acct_a.client.assume-role.arn                = arn:aws:iam::111111111111:role/glue-access
spark.sql.catalog.acct_a.hadoop.fs.s3a.assumed.role.arn        = arn:aws:iam::111111111111:role/data-access
```

The explicit key wins over the derived one. The session name, duration and external ID still come from the catalog's
role options, and the base credentials of S3A's STS call from the session. If the data role needs a different external
ID, set `hadoop.fs.s3a.assumed.role.external.id` too (Hadoop 3.4 only). Trino offers the same split with
`hive.metastore.glue.iam-role` and `s3.iam-role`.

On Spark 3.5 (Hadoop 3.3), S3A cannot send an external ID. A catalog with `client.assume-role.external-id` then gets no
derived S3A role at all, and gluecat logs one warning per catalog. Setting `hadoop.fs.s3a.assumed.role.arn` to a data
role whose trust policy has no external ID condition, as above, is the remedy (or use Spark 4.x).

### Key mapping

| Setting | Trino (Hive connector) | Iceberg `GlueCatalog` | gluecat |
|---|---|---|---|
| Glue catalog ID | `hive.metastore.glue.catalogid` | `glue.id` | `glue.id` |
| Region | `hive.metastore.glue.region` | `client.region` | `client.region` |
| Role for Glue | `hive.metastore.glue.iam-role` | `client.assume-role.arn`, with `client.factory=org.apache.iceberg.aws.AssumeRoleAwsClientFactory` | `client.assume-role.arn` |
| Region of the STS call | `hive.metastore.glue.sts.region`, `s3.sts.region` | `client.assume-role.region` | `client.assume-role.region` (Glue); `hadoop.fs.s3a.assumed.role.sts.endpoint` and `.sts.endpoint.region` for S3A |
| External ID | `hive.metastore.glue.external-id` | `client.assume-role.external-id` | `client.assume-role.external-id` |
| Role for S3 | `s3.iam-role` | the Glue role (same factory) | the Glue role by default; `hadoop.fs.s3a.assumed.role.arn` for another |

## 6. A worked example

Two catalogs: `acct_a` in account `111111111111` (eu-central-1) and `acct_b` in account `222222222222` (us-east-1).
The application runs as `arn:aws:iam::999999999999:role/spark-app`.

Spark configuration:

```
spark.sql.extensions                                  = io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog                       = org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.hadoop.fs.s3.impl                               = org.apache.hadoop.fs.s3a.S3AFileSystem

spark.sql.catalog.acct_a                              = io.github.sdaberdaku.gluecat.GlueCatalog
spark.sql.catalog.acct_a.glue.id                      = 111111111111
spark.sql.catalog.acct_a.client.region                = eu-central-1
spark.sql.catalog.acct_a.client.assume-role.arn       = arn:aws:iam::111111111111:role/glue-access
spark.sql.catalog.acct_a.warehouse                    = s3://bucket-a/warehouse

spark.sql.catalog.acct_b                              = io.github.sdaberdaku.gluecat.GlueCatalog
spark.sql.catalog.acct_b.glue.id                      = 222222222222
spark.sql.catalog.acct_b.client.region                = us-east-1
spark.sql.catalog.acct_b.client.assume-role.arn       = arn:aws:iam::222222222222:role/glue-access
spark.sql.catalog.acct_b.client.assume-role.external-id = example-external-id
spark.sql.catalog.acct_b.warehouse                    = s3://bucket-b/warehouse
```

This example needs Spark 4.x because `acct_b` uses an external ID, which Hadoop 3.3 (Spark 3.5) cannot send to STS; on
Spark 3.5 drop the external ID from the trust policy and the configuration, or use a data role without it as above.

If the application authenticates with IRSA, also set the session's `spark.hadoop.fs.s3a.aws.credentials.provider` to
the web-identity provider: `software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider` on
Spark 4.x (SDK v2), `com.amazonaws.auth.WebIdentityTokenCredentialsProvider` on Spark 3.5 (SDK v1). gluecat derives it
into the base credentials of S3A's STS call.

The identity policy of `spark-app` (account `999999999999`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": [
        "arn:aws:iam::111111111111:role/glue-access",
        "arn:aws:iam::222222222222:role/glue-access"
      ]
    }
  ]
}
```

The trust policy of `glue-access` in account `222222222222` (the one in `111111111111` is the same without the
condition):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::999999999999:role/spark-app" },
      "Action": "sts:AssumeRole",
      "Condition": { "StringEquals": { "sts:ExternalId": "example-external-id" } }
    }
  ]
}
```

The Glue policy of `glue-access`, scoped to the catalog of account `222222222222` in `us-east-1` (the unscoped
version is on the [Operations](operations.md#iam-permissions) page). Check a narrowed policy against AWS's
action-to-resource table before relying on it:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:CreateDatabase", "glue:GetDatabase", "glue:GetDatabases", "glue:UpdateDatabase", "glue:DeleteDatabase",
        "glue:CreateTable", "glue:GetTable", "glue:GetTables", "glue:UpdateTable", "glue:DeleteTable"
      ],
      "Resource": [
        "arn:aws:glue:us-east-1:222222222222:catalog",
        "arn:aws:glue:us-east-1:222222222222:database/*",
        "arn:aws:glue:us-east-1:222222222222:table/*/*",
        "arn:aws:glue:us-east-1:222222222222:userDefinedFunction/*/*"
      ]
    }
  ]
}
```

The S3 policy of the same role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload",
                 "s3:ListMultipartUploadParts"],
      "Resource": "arn:aws:s3:::bucket-b/*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:GetBucketLocation"],
      "Resource": "arn:aws:s3:::bucket-b"
    }
  ]
}
```

## 7. Troubleshooting

The exact wording varies by service and SDK version.

| Message | Meaning |
|---|---|
| STS: `... is not authorized to perform: sts:AssumeRole on resource: <role>` | Either the base identity's policy does not allow `sts:AssumeRole` on that role, or the role's trust policy does not name the base identity (or its external ID condition fails). Check both. |
| STS: `... Not authorized to perform sts:AssumeRole` raised by S3A (`AssumedRoleCredentialProvider`) | The same, for the S3A call. S3A uses the session's S3A credentials provider as its base identity, not the SDK default chain. |
| Glue: `... is not authorized to perform: glue:<Action> on resource: arn:aws:glue:...` | The role's Glue policy lacks the action, or the `Resource` list lacks the catalog, database or table ARN. gluecat reports it as `Catalog <name> (glue.id=...): access denied`. |
| Glue access denied for a `glue.id` of another account, with no role configured | The target account's Glue resource policy does not allow the caller. |
| S3: `403 AccessDenied` on `s3a://bucket-b/...` | The role's S3 policy or the bucket policy does not allow the request, or the bucket was first opened through another route (see the cache rule in section 5). |
| S3: `301 Moved Permanently` or `PermanentRedirect` | S3A is pinned to another region than the bucket's, or a custom endpoint names none. Set `hadoop.fs.s3a.endpoint.region` to the bucket's region, or remove the pin. |
| `ExpiredToken` or `The security token included in the request is expired` | The session ended and could not be refreshed, or a static key under `hadoop.*` expired. Prefer roles. |
| `http-client.type '<type>' needs software.amazon.awssdk:<artifact> on the classpath (missing class <class>)` | Add `apache-client` or `url-connection-client` (2.24.6) to the classpath. |
