package io.github.sdaberdaku.gluecat.it

import java.net.URI

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.delta.DeltaLog
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import software.amazon.awssdk.services.s3.model.S3Exception

/** Per-catalog `hadoop.*` options and the inherited role against MiniStack S3, while the session's S3A endpoint is
  * unreachable. Hadoop's FileSystem cache stays on, as in production: every catalog and every assertion has its own
  * bucket, so the first touch of each bucket is the one under test. */
@RunWith(classOf[JUnitRunner])
class HadoopOptionsIT extends SparkIT(Named) {
  private val S3 = Map(
    "hadoop.fs.s3a.endpoint" -> MiniStack.Endpoint,
    "hadoop.fs.s3a.path.style.access" -> "true",
    "hadoop.fs.s3a.connection.ssl.enabled" -> "false")

  // hadoop-aws 3.3 (Spark 3.5, SDK v1) needs the STS endpoint to reach MiniStack. hadoop-aws 3.4 (Spark 4.x) builds a
  // configured STS endpoint with the SDK bundle's shaded URIBuilder, which these modules exclude; there StsToMiniStack,
  // which redirects every SDK v2 STS client in the test JVM, sends S3A's STS calls to MiniStack.
  private val StsEndpoint =
    if (org.apache.spark.SPARK_VERSION.startsWith("3.")) Map(
      "hadoop.fs.s3a.assumed.role.sts.endpoint" -> MiniStack.Endpoint,
      "hadoop.fs.s3a.assumed.role.sts.endpoint.region" -> "eu-central-1")
    else Map.empty[String, String]

  // The session leaves fs.s3a.aws.credentials.provider unset, so no base provider is derived for the role catalogs:
  // S3A's default base (SimpleAWSCredentialsProvider, fs.s3a.access.key) is the identity that assumes the role.
  override protected def sessionConf: Map[String, String] = Map(
    "spark.hadoop.fs.s3a.endpoint" -> "http://127.0.0.1:1",
    "spark.hadoop.fs.s3a.path.style.access" -> "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled" -> "false",
    "spark.hadoop.fs.s3a.access.key" -> "111111111111",
    "spark.hadoop.fs.s3a.secret.key" -> "test",
    "spark.hadoop.fs.s3a.connection.maximum" -> "16",
    "spark.hadoop.fs.s3a.attempts.maximum" -> "1",
    "spark.hadoop.fs.s3a.retry.limit" -> "1")

  override protected def otherCatalogs: Map[String, Map[String, String]] = {
    val role = S3 ++ Map(
      "glue.id" -> "222222222222",
      "client.assume-role.arn" -> IamFixture.ensureRole("222222222222", "data-access",
        actions = IamFixture.GlueActions ++ Seq("s3:*"), trustedAccounts = Seq("111111111111")),
      "client.assume-role.region" -> "eu-central-1") ++ StsEndpoint
    Map(
      // No role: explicit hadoop.* keys only, over the session's base keys (the fixture role has no S3 actions).
      // Without a role nothing derives fs.s3a.endpoint.region, and SDK v1 (Spark 3.5) would sign for us-east-1.
      "acct_s3" -> (S3 ++ Map("client.assume-role.arn" -> "", "hadoop.fs.s3a.endpoint.region" -> "eu-central-1",
        "warehouse" -> "s3a://wh-a/warehouse")),
      "acct_role_s3" -> (role + ("warehouse" -> "s3a://wh-b/warehouse")),
      "acct_role_off" -> (role ++ Map(
        "hadoop.fs.s3a.aws.credentials.provider" -> "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "warehouse" -> "s3a://wh-a2/warehouse")))
  }

  private val glueB = new GlueInspector("222222222222")

  override def beforeAll(): Unit = {
    super.beforeAll() // resets MiniStack
    Seq("wh-a", "wh-p", "wh-c", "wh-a2").foreach(S3Fixture.createBucket("111111111111", _))
    S3Fixture.createBucket("222222222222", "wh-b")
  }

  override def afterAll(): Unit =
    try Seq("wh-a", "wh-p", "wh-c", "wh-a2", "wh-b", "direct-a").foreach(evict)
    finally try glueB.close()
    finally super.afterAll()

  /** Makes the next statement the first touch of `bucket` in this JVM: closes Hadoop's cached FileSystem for it (the
    * cache key is scheme, authority and user, not the configuration, so `get` returns the instance in use) and drops
    * Delta's cached logs. */
  private def evict(bucket: String): Unit = {
    FileSystem.get(URI.create(s"s3a://$bucket/"), spark.sparkContext.hadoopConfiguration).close()
    DeltaLog.clearCache()
  }

  private def count(sql: String): Long = spark.sql(sql).head().getLong(0)

  test("Delta through the catalog uses the catalog's S3A endpoint while the session's is unreachable") {
    spark.sql("CREATE DATABASE acct_s3.db")
    spark.sql("CREATE TABLE acct_s3.db.t USING delta AS SELECT 1 AS id")
    // Each statement below opens wh-a afresh, through the loaded table's storage properties, not the CTAS's instance.
    evict("wh-a")
    spark.sql("INSERT INTO acct_s3.db.t VALUES (2)")
    evict("wh-a")
    assert(count("SELECT count(*) FROM acct_s3.db.t") == 2)
    evict("wh-a")
    assert(count("SELECT count(*) FROM acct_s3.db.t VERSION AS OF 0") == 1)
    evict("wh-a")
    spark.sql("REFRESH TABLE acct_s3.db.t")
    assert(S3Fixture.listKeys("111111111111", "wh-a", "warehouse/db.db/t/_delta_log/")
      .exists(_.endsWith("00000000000000000001.json")))
  }

  test("parquet through the catalog too") {
    // Own bucket: the create does not touch it, so the insert is the first touch, through the storage properties.
    spark.sql("CREATE TABLE acct_s3.db.p (id INT) USING parquet LOCATION 's3a://wh-p/p'")
    spark.sql("INSERT INTO acct_s3.db.p VALUES (1)")
    assert(spark.sql("SELECT * FROM acct_s3.db.p").count() == 1)
    assert(S3Fixture.listKeys("111111111111", "wh-p", "p/").exists(_.endsWith(".parquet")))
  }

  test("a plain Delta CREATE opens its own bucket with the catalog's options first") {
    // Delta's by-path create takes no write options, so only gluecat's priming of the FileSystem cache reaches it.
    spark.sql("CREATE TABLE acct_s3.db.c (id INT) USING delta LOCATION 's3a://wh-c/c'")
    spark.sql("INSERT INTO acct_s3.db.c VALUES (1)")
    assert(count("SELECT count(*) FROM acct_s3.db.c") == 1)
    assert(S3Fixture.listKeys("111111111111", "wh-c", "c/_delta_log/").exists(_.endsWith("00000000000000000000.json")))
  }

  test("a direct path read in a bucket never touched through a catalog follows the session configuration and fails") {
    S3Fixture.createBucket("111111111111", "direct-a")
    val e = intercept[Exception](spark.read.parquet("s3a://direct-a/nothing").count())
    assert(fullMessage(e).contains("127.0.0.1:1") ||
      e.isInstanceOf[AnalysisException] && fullMessage(e).toLowerCase.contains("connect"), fullMessage(e))
  }

  test("the inherited role: data lands in the role's account bucket, invisible to the base identity") {
    spark.sql("CREATE DATABASE acct_role_s3.db")
    spark.sql("CREATE TABLE acct_role_s3.db.t USING delta AS SELECT 1 AS id")
    evict("wh-b") // the read opens wh-b through the loaded table's storage properties
    assert(count("SELECT count(*) FROM acct_role_s3.db.t") == 1)
    assert(S3Fixture.listKeys("222222222222", "wh-b", "warehouse/db.db/t/_delta_log/").nonEmpty)
    // Another account's bucket: MiniStack answers 403, as AWS does.
    assert(intercept[S3Exception](S3Fixture.listKeys("111111111111", "wh-b", "")).statusCode() == 403)
  }

  test("an explicit hadoop.* provider turns the inherited role off") {
    // Glue still goes through the role (account 222222222222); S3 uses the base keys, so wh-a2 of 111111111111.
    spark.sql("CREATE DATABASE acct_role_off.db_off")
    spark.sql("CREATE TABLE acct_role_off.db_off.t USING delta AS SELECT 1 AS id")
    evict("wh-a2")
    assert(count("SELECT count(*) FROM acct_role_off.db_off.t") == 1)
    assert(glueB.listTableNames("db_off") == Seq("t"))
    assert(S3Fixture.listKeys("111111111111", "wh-a2", "warehouse/db_off.db/t/_delta_log/").nonEmpty)
  }

  test("the Glue entry has no fs.* parameters") {
    Seq(glue.getTable("db", "t"), glueB.getTable("db", "t")).foreach { t => // acct_s3's and acct_role_s3's
      val keys = t.parameters().keySet().asScala ++ t.storageDescriptor().serdeInfo().parameters().keySet().asScala
      assert(!keys.exists(_.contains("fs.s3a.")), s"${t.name()}: $keys")
    }
  }
}
