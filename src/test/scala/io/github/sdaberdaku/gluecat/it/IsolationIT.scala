package io.github.sdaberdaku.gluecat.it

import scala.collection.JavaConverters._

import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import io.github.sdaberdaku.gluecat.glue.StaticKeyCredentialsProvider

object IsolationIT {
  val HookConf = "spark.databricks.delta.catalog.update.enabled"

  val Writes = Set("CreateTable", "UpdateTable", "DeleteTable", "CreateDatabase", "UpdateDatabase", "DeleteDatabase")

  /** A catalog on account 999 through the static-key provider, so nothing else can reach that account. */
  val Account999: Map[String, String] = Map(
    "glue.id" -> "999999999999",
    "client.credentials-provider" -> classOf[StaticKeyCredentialsProvider].getName,
    "client.credentials-provider.access-key" -> "999999999999",
    "client.credentials-provider.secret-key" -> "test",
    "client.assume-role.arn" -> "")
}

/** `acct_a` (account 111 through the fixture's role), `acct_b` (account 222 through a role trusting 111) and a
  * `spark_catalog` on account 999 in one session. */
@RunWith(classOf[JUnitRunner])
class IsolationIT extends SparkIT(Named) {
  import IsolationIT._

  override protected def otherCatalogs: Map[String, Map[String, String]] = Map(
    "acct_b" -> Map(
      "glue.id" -> "222222222222",
      "client.assume-role.arn" -> IamFixture.ensureRole("222222222222", "glue-access", trustedAccounts = Seq("111111111111"))),
    "spark_catalog" -> Account999)

  private val glueB = new GlueInspector("222222222222")
  private val glue999 = new GlueInspector("999999999999")

  override def afterAll(): Unit =
    try { glueB.close(); glue999.close() }
    finally super.afterAll()

  private def describedColumns(table: String): Seq[String] =
    spark.sql(s"DESCRIBE $table").collect().map(_.getString(0)).filterNot(c => c.isEmpty || c.startsWith("#")).toList

  test("same db.table in two accounts stays separate") {
    spark.sql("CREATE DATABASE acct_a.db")
    spark.sql("CREATE DATABASE acct_b.db")
    spark.sql(s"CREATE TABLE acct_a.db.t (only_in_a INT) USING delta LOCATION '${tempDir()}'")
    spark.sql(s"CREATE TABLE acct_b.db.t (only_in_b STRING) USING delta LOCATION '${tempDir()}'")
    assert(describedColumns("acct_a.db.t") == Seq("only_in_a"))
    assert(describedColumns("acct_b.db.t") == Seq("only_in_b"))
    def glueColumns(inspector: GlueInspector) =
      inspector.getTable("db", "t").storageDescriptor().columns().asScala.map(_.name()).toList
    assert(glueColumns(glue) == Seq("only_in_a"))
    assert(glueColumns(glueB) == Seq("only_in_b"))
    assert(glue999.listDatabaseNames().isEmpty)
  }

  test("every recorded request carried its own CatalogId") {
    Seq("acct_a", "acct_b").foreach(c => spark.sql(s"CREATE DATABASE $c.db_w"))
    Seq("acct_a", "acct_b", "spark_catalog").foreach(c => spark.sql(s"SHOW NAMESPACES IN $c").collect())
    val recorded = TestGlueCatalog.recorded.asScala.toList
    def of(catalog: String) = recorded.filter(_._1 == catalog)
    assert(of("acct_a").exists(r => Writes(r._2)) && of("acct_a").forall(_._3.contains("111111111111")))
    assert(of("acct_b").exists(r => Writes(r._2)) && of("acct_b").forall(_._3.contains("222222222222")))
    assert(of("spark_catalog").nonEmpty && of("spark_catalog").forall(_._3.contains("999999999999")))
    assert(of("spark_catalog").map(_._2).filter(Writes).isEmpty)
  }

  test("UpdateCatalog hook on makes initialize fail") {
    startSession()
    try {
      spark.conf.set(HookConf, "true")
      val e = intercept[Exception](spark.sql("SHOW NAMESPACES IN acct_a").collect())
      val message = fullMessage(e)
      assert(message.contains("catalog.update.enabled"))
      assert(message.contains("gluecat catalog 'acct_a'"))
    } finally startSession()
  }

  test("UpdateCatalog hook turned on at runtime is refused before every Delta operation by name") {
    spark.sql("CREATE DATABASE acct_a.hook")
    spark.sql(s"CREATE TABLE acct_a.hook.t (id INT) USING delta LOCATION '${tempDir()}'")
    spark.sql("INSERT INTO acct_a.hook.t VALUES (1)")
    try {
      spark.conf.set(HookConf, "true")
      Seq("INSERT INTO acct_a.hook.t VALUES (2)", "SELECT * FROM acct_a.hook.t").foreach { q =>
        assert(fullMessage(intercept[Exception](spark.sql(q).collect())).contains("catalog.update.enabled"), q)
      }
      spark.conf.set(HookConf, "false")
      spark.sql("INSERT INTO acct_a.hook.t VALUES (3)")
      assert(spark.table("acct_a.hook.t").count() == 2)
    } finally spark.conf.unset(HookConf)
  }
}
