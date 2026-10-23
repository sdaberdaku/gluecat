# Quick start

## Dependencies

gluecat needs the AWS SDK v2 modules `glue` and `sts` on the Spark classpath; the jar contains no SDK classes.

- Spark 3.5 (Hadoop 3.3, whose S3A uses SDK v1): add them yourself, for example
  `--packages software.amazon.awssdk:glue:2.24.6,software.amazon.awssdk:sts:2.24.6`.
- Spark 4.x: when `hadoop-aws` (S3A) is on the classpath, the `software.amazon.awssdk:bundle` it depends on already
  contains both. Otherwise add them as for Spark 3.5.

Example for Spark 4.1 with Delta 4.4.1 and S3A: `hadoop-aws` (the version of Spark's Hadoop, 3.4.2 for Spark 4.1.3)
brings the SDK bundle, and mapping `s3://` to S3A keeps the table locations in Glue as `s3://`, the form Athena reads:

```
spark-sql --packages io.github.sdaberdaku:gluecat-spark-4.1_2.13:1.1.0,io.delta:delta-spark_4.1_2.13:4.4.1,org.apache.hadoop:hadoop-aws:3.4.2 \
  --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

## As named catalogs

One named Spark catalog per Glue catalog (`spark-defaults.conf` or `--conf`; keep comments on their own lines):

```
spark.sql.extensions                                     = io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog                          = org.apache.spark.sql.delta.catalog.DeltaCatalog

spark.sql.catalog.acct_a                                 = io.github.sdaberdaku.gluecat.GlueCatalog
spark.sql.catalog.acct_a.glue.id                         = 111111111111
spark.sql.catalog.acct_a.client.region                   = eu-central-1
spark.sql.catalog.acct_a.client.assume-role.arn          = arn:aws:iam::111111111111:role/glue-access
# optional
spark.sql.catalog.acct_a.http-client.type                = urlconnection
spark.sql.catalog.acct_a.hadoop.fs.s3a.connection.maximum = 100
spark.sql.catalog.acct_a.warehouse                       = s3://bucket-a/warehouse

spark.sql.catalog.acct_b                                 = io.github.sdaberdaku.gluecat.GlueCatalog
spark.sql.catalog.acct_b.glue.id                         = 222222222222
spark.sql.catalog.acct_b.client.region                   = eu-central-1
spark.sql.catalog.acct_b.client.assume-role.arn          = arn:aws:iam::222222222222:role/glue-access
```

The first two lines are Delta's usual session configuration; Delta's SQL commands (`MERGE`, `UPDATE`, `DELETE`,
`OPTIMIZE`, `VACUUM`, ...) need the extension.

```sql
CREATE DATABASE acct_a.db LOCATION 's3://bucket-a/warehouse/db.db';
CREATE TABLE acct_a.db.events (id BIGINT, kind STRING) USING delta;
INSERT INTO acct_a.db.events VALUES (1, 'a');
CREATE DATABASE acct_b.db;
CREATE TABLE acct_b.db.events USING delta LOCATION 's3://bucket-b/db/events' AS SELECT * FROM acct_a.db.events;
```

A catalog never calls any Glue catalog other than its own `glue.id`.

S3 access follows the catalog role: with `client.assume-role.arn`, S3A assumes the same role for the files of the
catalog's tables, so the session needs no per-bucket S3A settings. `hadoop.<key>` options override that per catalog.
`http-client.type = urlconnection` needs `software.amazon.awssdk:url-connection-client` on the classpath. For several
accounts and regions, roles, trust policies and troubleshooting, see [Across AWS accounts](multi-account.md).

## As `spark_catalog`

`GlueCatalog` can replace the built-in session catalog, so unqualified `db.table` names and tools that assume
`spark_catalog` go to Glue without the Hive client:

```
spark.sql.extensions                          = io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalogImplementation               = in-memory
spark.sql.catalog.spark_catalog               = io.github.sdaberdaku.gluecat.GlueCatalog
spark.sql.catalog.spark_catalog.glue.id       = 111111111111
spark.sql.catalog.spark_catalog.client.region = eu-central-1
# more catalogs as usual
spark.sql.catalog.acct_b                      = io.github.sdaberdaku.gluecat.GlueCatalog
spark.sql.catalog.acct_b.glue.id              = 222222222222
```

`spark.sql.catalogImplementation = in-memory` is required. Spark's in-memory catalog stays in place for temporary
views and functions, which work as usual. Spark also sends `CREATE VIEW` and `CREATE FUNCTION` to that in-memory
catalog, which has only a `default` database: a view in a Glue database fails with `SCHEMA_NOT_FOUND`, and a view in
`default` exists in memory only, for the lifetime of the application. Nothing of this reaches Glue. The operation
tests run in both modes; the credentials, account isolation and concurrency tests use named catalogs.

As `spark_catalog` on Spark 4.1 and later, path-based Delta DDL makes Spark call `GetDatabase` for a database named
`delta`; see [Limitations](limitations.md).
