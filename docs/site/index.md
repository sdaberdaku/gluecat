# gluecat

AWS Glue Data Catalog client for Apache Spark.

gluecat is a Spark catalog plugin, `io.github.sdaberdaku.gluecat.GlueCatalog`. Each Spark catalog configured with it
talks to one Glue Data Catalog, with its own catalog ID and its own credentials or IAM role. It loads, creates and
alters Delta tables there, reads other data source tables, and manages Glue databases. It works as a named catalog
and as `spark_catalog`. It does not use the Hive metastore client, needs no patched Spark or Hive jars, and uses the
AWS SDK for Java v2.

API documentation: [Scaladoc](https://sdaberdaku.github.io/gluecat/api/).

## Why

Out of the box, Delta Lake works only through Spark's built-in session catalog, `spark_catalog`, and the session
catalog sits on a single Hive metastore client. With the AWS Glue Hive client, that means one Glue catalog ID and one
set of credentials for the whole Spark application.

When each customer, team or environment has its own AWS account, the S3 data can already live in each account (S3A
handles per-bucket access), but every Delta table still has to be registered in one shared Glue catalog. A single
Spark application, for example a Spark Connect server that runs jobs for several accounts, cannot register account
A's tables in account A's Glue and account B's tables in account B's Glue.

Iceberg's `GlueCatalog` already gives each named catalog its own Glue catalog ID and IAM role. gluecat brings that
model to Glue tables in general and to Delta tables in particular. Its option names follow Iceberg's AWS properties.

## Artifacts

One artifact per Spark minor version and Scala version, group `io.github.sdaberdaku`:

| Artifact | Spark | Scala | Java bytecode | JDKs tested | Delta tested |
|---|---|---|---|---|---|
| `gluecat-spark-3.5_2.12` | 3.5.3 and later 3.5 | 2.12 | 8 | 8, 11, 17 | 3.3.3 |
| `gluecat-spark-3.5_2.13` | 3.5.3 and later 3.5 | 2.13 | 8 | 8, 11, 17 | 3.3.3 |
| `gluecat-spark-4.0_2.13` | 4.0.x | 2.13 | 17 | 17, 21 | 4.0.1, 4.1.0, 4.2.0, 4.3.1, 4.4.1 |
| `gluecat-spark-4.1_2.13` | 4.1.x | 2.13 | 17 | 17, 21 | 4.1.0 and 4.2.0 (on Spark 4.1.1), 4.3.1, 4.4.1 |
| `gluecat-spark-4.2_2.13` | 4.2.x | 2.13 | 17 | 17, 21, 25 | 4.4.1 |

Spark 3.5.0 to 3.5.2 are not supported: they reject the tables this catalog returns for non-Delta data, and
`initialize` fails on them. `matrix.json` lists every Spark, Delta and JDK combination CI runs.

Delta is optional: without Delta on the classpath, the catalog serves non-Delta tables only.

## Where to go next

- [Quick start](quickstart.md): dependencies and configuration.
- [Configuration](configuration.md): every option, credentials, AWS SDK requirements.
- [Operations](operations.md): what works, for Delta and for other tables.
- [Glue entry format](glue-entry-format.md): what the catalog writes to Glue and which readers it works with.
- [How it works](how-it-works.md): why Delta creates run by path.
- [Limitations](limitations.md).
